"""Apify -> Supabase -> Telegram. One Render instance; no X polling."""
import asyncio
import hmac
import logging
import os
import re
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

import aiohttp
from aiohttp import web
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError, TelegramRetryAfter

log = logging.getLogger(__name__)
_ALLOWED = {'superex', 'superexpersian'}
_ID = re.compile(r'^[A-Za-z0-9_-]{1,100}$')


def parse_item(row):
    if not isinstance(row, dict):
        raise ValueError('Dataset row is not an object')
    # Actor diagnostic rows are not tweets.
    if row.get('status') == 'zero-output' or str(row.get('id', '')).startswith('diag:'):
        return None
    flags = ('isRetweet', 'isReply', 'isQuoteStatus')
    if any(type(row.get(k)) is not bool for k in flags):
        raise ValueError('Missing or invalid tweet type flags')
    if any(row[k] for k in flags):
        return None
    author = row.get('author')
    if not isinstance(author, dict) or not isinstance(author.get('username'), str):
        raise ValueError('Missing author.username')
    username = author['username']
    if username.lower() not in _ALLOWED:
        return None
    tweet_id, text, created = row.get('id'), row.get('text'), row.get('createdAt')
    if not isinstance(tweet_id, str) or not tweet_id.isdigit() or not isinstance(text, str) or not isinstance(created, str):
        raise ValueError('Invalid tweet id, text, or timestamp')
    try:
        stamp = parsedate_to_datetime(created)
        if stamp.tzinfo is None:
            raise ValueError('timezone missing')
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError('Invalid createdAt') from exc
    return {'id': tweet_id, 'text': text, 'username': username,
            'time': stamp.astimezone(timezone.utc),
            'url': f'https://x.com/{username}/status/{tweet_id}'}


def message_for(item):
    suffix = '\n\n' + item['url']
    prefix = '@' + item['username'] + '\n\n'
    text = item['text'].strip()
    limit = 3900 - len(prefix) - len(suffix)
    if len(text) > limit:
        text = text[:limit - 1] + '…'
    return prefix + text + suffix


class TweetsFeature:
    def __init__(self, bot, session):
        self.bot, self.http = bot, session
        self.secret = os.getenv('APIFY_WEBHOOK_SECRET', '').strip()
        self.token = os.getenv('APIFY_TOKEN', '').strip()
        self.url = os.getenv('SUPABASE_URL', '').strip().rstrip('/')
        self.key = os.getenv('SUPABASE_KEY', '').strip()
        self.chat = int(os.environ['TWEETS_CHAT_ID'])
        self.admin = int(os.environ['ADMIN_CHAT_ID'])
        self.seed = os.getenv('TWEETS_SEED_MODE', 'true').lower() in ('1', 'true', 'yes', 'on')
        if not all((self.secret, self.token, self.url, self.key)):
            raise ValueError('Missing tweet configuration')
        self.headers = {'apikey': self.key, 'Authorization': 'Bearer ' + self.key}
        self.task = None
        self.wakeup = asyncio.Event()
        self.started = datetime.now(timezone.utc)
        self.last_alert = None

    async def db(self, path, method='GET', **kwargs):
        headers = dict(self.headers)
        headers.update(kwargs.pop('headers', {}))
        timeout = aiohttp.ClientTimeout(total=12)
        async with self.http.request(method, self.url + '/rest/v1/' + path,
                                     headers=headers, timeout=timeout, **kwargs) as r:
            body = await r.text()
            if r.status >= 300:
                raise RuntimeError(f'Supabase HTTP {r.status}')
            return await r.json(content_type=None) if body else None

    async def rpc(self, name, payload):
        return await self.db('rpc/' + name, 'POST', json=payload)

    async def alert(self, message):
        try:
            await self.bot.send_message(self.admin, message, parse_mode=None)
            return True
        except Exception:
            log.exception('Failed to send admin alert')
            return False

    async def webhook(self, request):
        if not hmac.compare_digest(request.headers.get('X-Apify-Secret', ''), self.secret):
            return web.Response(status=401, text='Unauthorized')
        try:
            p = await request.json()
            event, res = p['eventType'], p['resource']
            run_id = res['id']
            if not isinstance(run_id, str) or not _ID.fullmatch(run_id):
                raise ValueError('invalid run id')
            if event == 'ACTOR.RUN.SUCCEEDED':
                dataset = res['defaultDatasetId']
                if not isinstance(dataset, str) or not _ID.fullmatch(dataset):
                    raise ValueError('invalid dataset id')
            elif event in ('ACTOR.RUN.FAILED', 'ACTOR.RUN.TIMED_OUT', 'ACTOR.RUN.ABORTED'):
                dataset = None
            else:
                return web.Response(status=400, text='Unsupported event')
            await self.rpc('tweets_enqueue', {'p_run_id': run_id, 'p_dataset': dataset, 'p_event': event})
        except (ValueError, KeyError, TypeError):
            return web.Response(status=400, text='Invalid payload')
        except Exception:
            log.exception('Could not persist webhook')
            return web.Response(status=503, text='Retry later')
        self.wakeup.set()
        return web.Response(status=200, text='Accepted')

    async def fetch_dataset(self, dataset):
        url = f'https://api.apify.com/v2/datasets/{dataset}/items'
        async with self.http.get(url, headers={'Authorization': 'Bearer ' + self.token},
                                 params={'format': 'json', 'clean': 'false'},
                                 timeout=aiohttp.ClientTimeout(total=30)) as r:
            if r.status != 200:
                raise RuntimeError(f'Apify dataset HTTP {r.status}')
            data = await r.json(content_type=None)
        if not isinstance(data, list):
            raise ValueError('Dataset is not a list')
        return data

    async def process(self, job):
        if job['event_type'] != 'ACTOR.RUN.SUCCEEDED':
            if not await self.alert('⚠️ اجرای Apify ناموفق بود: ' + job['event_type'] + '\nRun: ' + job['run_id']):
                raise RuntimeError('Admin alert failed')
            return
        rows = await self.fetch_dataset(job['dataset_id'])
        tweets = [t for row in rows if (t := parse_item(row)) is not None]
        tweets.sort(key=lambda t: (t['time'], int(t['id'])))
        # Atomic initial seed, including an empty first successful dataset.
        seeded = await self.rpc('tweets_initialize', {'p_ids': [t['id'] for t in tweets], 'p_seed': self.seed})
        if seeded:
            log.info('Initial seed completed: %d tweets', len(tweets))
            return
        for tweet in tweets:
            claimed = await self.db('seen_tweets?on_conflict=tweet_id', 'POST',
                json={'tweet_id': tweet['id']},
                headers={'Prefer': 'resolution=ignore-duplicates,return=representation'})
            if not claimed:
                continue
            try:
                await self.bot.send_message(self.chat, message_for(tweet), parse_mode=None)
            except (TelegramBadRequest, TelegramForbiddenError, TelegramRetryAfter) as exc:
                # Explicit Telegram rejection: no message was accepted; release claim.
                await self.db('seen_tweets', 'DELETE', params={'tweet_id': 'eq.' + tweet['id']},
                              headers={'Prefer': 'return=minimal'})
                await self.alert('⚠️ ارسال توییت رد شد؛ در اجرای بعدی دوباره تلاش می‌شود. ID: ' + tweet['id'])
                log.warning('Telegram rejected tweet %s: %s', tweet['id'], type(exc).__name__)
            except Exception:
                # Unknown result (including timeout): keep claim to avoid duplicates.
                log.exception('Telegram result uncertain for tweet %s', tweet['id'])
                await self.alert('⚠️ نتیجه ارسال این توییت نامشخص است؛ برای جلوگیری از تکرار، خودکار دوباره ارسال نمی‌شود. ID: ' + tweet['id'])

    async def monitor(self):
        rows = await self.db('tweets_state', params={'id': 'eq.1', 'select': 'last_success_at,monitoring_since'})
        last = (rows[0]['last_success_at'] or rows[0]['monitoring_since']) if rows else None
        reference = datetime.fromisoformat(last.replace('Z', '+00:00')) if last else self.started
        now = datetime.now(timezone.utc)
        if (now - reference).total_seconds() >= 10800 and (not self.last_alert or (now - self.last_alert).total_seconds() >= 10800):
            if await self.alert('⚠️ بیش از ۳ ساعت وب‌هوک موفق Apify دریافت نشده است. وضعیت Render و Apify را بررسی کنید.'):
                self.last_alert = now
        elif (now - reference).total_seconds() < 10800:
            self.last_alert = None

    async def run(self):
        while True:
            try:
                job = await self.rpc('tweets_claim_job', {})
                if job:
                    try:
                        await self.process(job)
                        await self.rpc('tweets_finish_job', {'p_run_id': job['run_id'], 'p_ok': True})
                    except Exception:
                        log.exception('Tweet job failed: %s', job['run_id'])
                        await self.alert('⚠️ پردازش دیتاست Apify ناموفق بود؛ دوباره تلاش می‌شود. Run: ' + job['run_id'])
                        await self.rpc('tweets_finish_job', {'p_run_id': job['run_id'], 'p_ok': False})
                else:
                    await self.monitor()
                    self.wakeup.clear()
                    try:
                        await asyncio.wait_for(self.wakeup.wait(), timeout=60)
                    except asyncio.TimeoutError:
                        pass
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception('Tweets worker error')
                await asyncio.sleep(60)

    async def start(self, app):
        self.task = asyncio.create_task(self.run())

    async def stop(self, app):
        if self.task:
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass


def setup_tweets_feature(app, bot, session):
    feature = TweetsFeature(bot, session)
    app.router.add_post('/apify-webhook', feature.webhook)
    app.on_startup.append(feature.start)
    app.on_cleanup.append(feature.stop)
