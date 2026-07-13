#GHUMMMMMMM...A
#JOIN CHANNEL  https://t.me/bloodbrx98


from flask import Flask, request, jsonify
import json, os, aiohttp, asyncio, requests, binascii, time, threading
from datetime import datetime, timedelta
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad
from google.protobuf.json_format import MessageToJson
import like_pb2, like_count_pb2, uid_generator_pb2
from google.protobuf.message import DecodeError

app = Flask(__name__)

ACCOUNTS_FILE = 'accounts.json'
BATCH_SIZE = 21  # Har baar kitne accounts se token lena hai
CACHE_DURATION = 3600  # 1 hour

# ✅ In-Memory Cache (Vercel compatible)
class MemoryCache:
    def __init__(self):
        self.tokens = []
        self.batch_info = {}
        self.timestamp = 0
        self.expiry = 0
        self.current_index = 0
        self.all_accounts = []
        self.is_initialized = False
        self.total_cycles = 0  # Kitni baar complete cycle hui
        self.last_batch_was_partial = False  # Last batch partial thi?
    
    def is_valid(self):
        return self.is_initialized and time.time() < self.expiry and len(self.tokens) > 0
    
    def set_cache(self, tokens, batch_info):
        self.tokens = tokens
        self.batch_info = batch_info
        self.timestamp = time.time()
        self.expiry = time.time() + CACHE_DURATION
        self.is_initialized = True
    
    def get_cache(self):
        if self.is_valid():
            return self.tokens, self.batch_info
        return None, None
    
    def update_index(self, index):
        self.current_index = index
    
    def load_accounts(self):
        if os.path.exists(ACCOUNTS_FILE):
            with open(ACCOUNTS_FILE, 'r') as f:
                accounts = json.load(f)
                self.all_accounts = list(accounts.items())
                return self.all_accounts
        return []

# ✅ Global cache instance
cache = MemoryCache()

# ✅ Load accounts on startup
def load_accounts():
    return cache.load_accounts()

# ✅ Get next batch of accounts (with special logic)
def get_next_account_batch():
    if not cache.all_accounts:
        load_accounts()
    
    if not cache.all_accounts:
        return []
    
    total_accounts = len(cache.all_accounts)
    
    # 🔥 SPECIAL LOGIC: Agar last batch partial thi (16 accounts),
    # toh agli baar pehle 5 accounts se start karo
    if cache.last_batch_was_partial:
        print(f"🔄 Last batch was partial, starting from first 5 accounts")
        cache.current_index = 0
        cache.last_batch_was_partial = False
        cache.total_cycles += 1
    
    # Get next 21 accounts
    start_idx = cache.current_index
    end_idx = min(start_idx + BATCH_SIZE, total_accounts)
    batch = cache.all_accounts[start_idx:end_idx]
    
    # Check if this is the last batch (partial)
    if end_idx - start_idx < BATCH_SIZE and end_idx == total_accounts:
        cache.last_batch_was_partial = True
        print(f"⚠️ This is the last batch with only {len(batch)} accounts")
    else:
        cache.last_batch_was_partial = False
    
    # Update index for next time
    cache.current_index = end_idx
    
    # Agar end reach ho gaya, toh reset but keep flag
    if cache.current_index >= total_accounts:
        cache.current_index = 0
        # Last batch thi toh flag already set hai
        if not cache.last_batch_was_partial:
            cache.last_batch_was_partial = True
            cache.total_cycles += 1
    
    print(f"📊 Batch: Accounts {start_idx+1} to {end_idx} ({len(batch)} accounts)")
    return batch

# ✅ Fetch token from API
async def fetch_token(session, uid, password):
    url = f"https://jwtmc.vercel.app/token?uid={uid}&password={password}"
    try:
        async with session.get(url, timeout=10) as res:
            if res.status == 200:
                text = await res.text()
                try:
                    data = json.loads(text)
                    if isinstance(data, list) and len(data) > 0 and "token" in data[0]:
                        return data[0]["token"]
                    elif isinstance(data, dict) and "token" in data:
                        return data["token"]
                except:
                    return None
    except:
        return None
    return None

# ✅ Fetch tokens from current batch
async def fetch_batch_tokens():
    batch = get_next_account_batch()
    if not batch:
        return []
    
    account_numbers = [uid for uid, _ in batch]
    print(f"🔄 Fetching tokens for {len(batch)} accounts")
    print(f"📋 Account UIDs: {account_numbers[:5]}... (showing first 5)")
    
    tokens = []
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_token(session, uid, password) for uid, password in batch]
        results = await asyncio.gather(*tasks)
        tokens = [token for token in results if token]
    
    print(f"✅ Fetched {len(tokens)} valid tokens out of {len(batch)}")
    return tokens

# ✅ Get tokens (with auto-refresh)
async def get_tokens_live(force_refresh=False):
    # Try cache first
    if not force_refresh:
        cached_tokens, batch_info = cache.get_cache()
        if cached_tokens:
            print(f"✅ Using cached tokens: {len(cached_tokens)} tokens")
            return cached_tokens
    
    # Fetch fresh batch
    print("🔄 Fetching fresh tokens...")
    tokens = await fetch_batch_tokens()
    
    if tokens:
        # Calculate current batch info
        total = len(cache.all_accounts)
        current_pos = cache.current_index
        batch_num = (current_pos // BATCH_SIZE) + 1
        total_batches = (total + BATCH_SIZE - 1) // BATCH_SIZE
        
        batch_info = {
            'batch_number': batch_num,
            'total_batches': total_batches,
            'accounts_used': len(tokens),
            'total_accounts': total,
            'batch_range': f"{current_pos - len(tokens) + 1}-{current_pos}",
            'is_partial': cache.last_batch_was_partial,
            'total_cycles_completed': cache.total_cycles,
            'timestamp': datetime.now().isoformat()
        }
        cache.set_cache(tokens, batch_info)
        print(f"✅ Fresh tokens cached: {len(tokens)} tokens")
    else:
        print("❌ No tokens fetched, using old cache if available")
        cached_tokens, _ = cache.get_cache()
        if cached_tokens:
            return cached_tokens
    
    return tokens

# ✅ Encryption functions
def encrypt_message(plaintext):
    key = b'Yg&tc%DEuh6%Zc^8'
    iv = b'6oyZDr22E3ychjM%'
    cipher = AES.new(key, AES.MODE_CBC, iv)
    return binascii.hexlify(cipher.encrypt(pad(plaintext, AES.block_size))).decode()

def create_uid_proto(uid):
    pb = uid_generator_pb2.uid_generator()
    pb.saturn_ = int(uid)
    pb.garena = 1
    return pb.SerializeToString()

def create_like_proto(uid):
    pb = like_pb2.like()
    pb.uid = int(uid)
    return pb.SerializeToString()

def decode_protobuf(binary):
    try:
        pb = like_count_pb2.Info()
        pb.ParseFromString(binary)
        return pb
    except DecodeError:
        return None

def make_request(enc_uid, token):
    url = "https://client.ind.freefiremobile.com/GetPlayerPersonalShow"
    headers = {
        'User-Agent': "Dalvik/2.1.0 (Linux; U; Android 9; ASUS_Z01QD Build/PI)",
        'Connection': "Keep-Alive",
        'Accept-Encoding': "gzip",
        'Authorization': f"Bearer {token}",
        'Content-Type': "application/x-www-form-urlencoded",
        'Expect': "100-continue",
        'X-Unity-Version': "2018.4.11f1",
        'X-GA': "v1 1",
        'ReleaseVersion': "OB54"
    }
    try:
        res = requests.post(url, data=bytes.fromhex(enc_uid), headers=headers, verify=False)
        return decode_protobuf(res.content)
    except:
        return None

async def send_request(enc_uid, token):
    url = "https://client.ind.freefiremobile.com/LikeProfile"
    headers = {
        'User-Agent': "Dalvik/2.1.0 (Linux; U; Android 9; ASUS_Z01QD Build/PI)",
        'Connection': "Keep-Alive",
        'Accept-Encoding': "gzip",
        'Authorization': f"Bearer {token}",
        'Content-Type': "application/x-www-form-urlencoded",
        'Expect': "100-continue",
        'X-Unity-Version': "2018.4.11f1",
        'X-GA': "v1 1",
        'ReleaseVersion': "OB54"
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, data=bytes.fromhex(enc_uid), headers=headers, ssl=False) as r:
                return r.status
    except Exception as e:
        print(f"Error in send_request: {e}")
        return None

async def send_likes(uid, tokens):
    enc_uid = encrypt_message(create_like_proto(uid))
    tasks = [send_request(enc_uid, token) for token in tokens]
    return await asyncio.gather(*tasks)

# ✅ Main endpoint
@app.route('/like', methods=['GET'])
def like_handler():
    uid = request.args.get("uid")
    force_refresh = request.args.get("refresh", "false").lower() == "true"
    
    if not uid:
        return jsonify({"error": "Missing UID"}), 400

    try:
        # Get tokens
        tokens = asyncio.run(get_tokens_live(force_refresh))
        if not tokens:
            return jsonify({"error": "No valid tokens available"}), 401

        # Get player info before likes
        enc_uid = encrypt_message(create_uid_proto(uid))
        before = make_request(enc_uid, tokens[0])
        if not before:
            return jsonify({"error": "Failed to retrieve player info"}), 500

        before_data = json.loads(MessageToJson(before))
        likes_before = int(before_data.get("AccountInfo", {}).get("Likes", 0))
        nickname = before_data.get("AccountInfo", {}).get("PlayerNickname", "Unknown")

        # Send likes
        responses = asyncio.run(send_likes(uid, tokens))
        success_count = sum(1 for r in responses if r == 200)

        # Get player info after likes
        after = make_request(enc_uid, tokens[0])
        likes_after = likes_before
        if after:
            after_data = json.loads(MessageToJson(after))
            likes_after = int(after_data.get("AccountInfo", {}).get("Likes", 0))

        # Get cache info
        _, batch_info = cache.get_cache()
        
        return jsonify({
            "PlayerNickname": nickname,
            "UID": uid,
            "LikesBefore": likes_before,
            "LikesAfter": likes_after,
            "LikesGivenByAPI": likes_after - likes_before,
            "SuccessfulRequests": success_count,
            "TotalRequests": len(tokens),
            "TokensUsed": len(tokens),
            "BatchInfo": batch_info or {"message": "No batch info"},
            "CacheStatus": "Active" if cache.is_valid() else "Expired",
            "NextRefresh": "1 hour",
            "status": 1 if likes_after > likes_before else 2,
            "developer": "semy"
        })

    except Exception as e:
        return jsonify({"error": f"Internal server error: {str(e)}"}), 500

# ✅ Cache status endpoint
@app.route('/cache/status', methods=['GET'])
def cache_status():
    if cache.is_valid():
        _, batch_info = cache.get_cache()
        return jsonify({
            "status": "active",
            "tokens_count": len(cache.tokens),
            "batch_info": batch_info,
            "total_accounts": len(cache.all_accounts),
            "current_position": cache.current_index,
            "total_cycles": cache.total_cycles,
            "last_batch_partial": cache.last_batch_was_partial,
            "cache_duration": f"{CACHE_DURATION} seconds",
            "auto_refresh": "Active (every 1 hour)",
            "rotation": f"{BATCH_SIZE} accounts per batch",
            "memory_usage": "In-Memory (Vercel compatible)"
        })
    else:
        return jsonify({
            "status": "empty",
            "message": "No cache found or expired",
            "total_accounts": len(cache.all_accounts),
            "next_refresh": "Automatic on next request"
        })

# ✅ Force refresh endpoint
@app.route('/cache/refresh', methods=['POST'])
def refresh_cache():
    try:
        tokens = asyncio.run(get_tokens_live(force_refresh=True))
        if tokens:
            return jsonify({
                "status": "success",
                "tokens_fetched": len(tokens),
                "message": "Cache refreshed successfully ✅"
            })
        else:
            return jsonify({
                "status": "error",
                "message": "Failed to fetch new tokens"
            }), 500
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ✅ Batch info endpoint
@app.route('/batch/info', methods=['GET'])
def batch_info():
    total = len(cache.all_accounts)
    current = cache.current_index
    batch_num = (current // BATCH_SIZE) + 1
    total_batches = (total + BATCH_SIZE - 1) // BATCH_SIZE
    
    start_idx = current
    end_idx = min(start_idx + BATCH_SIZE, total)
    batch_size = end_idx - start_idx
    
    # Special logic for next batch
    next_start = end_idx
    if next_start >= total:
        if cache.last_batch_was_partial:
            next_start = 0  # Wapas se start
            next_end = min(BATCH_SIZE, total)  # Pehle 5 accounts
        else:
            next_start = 0
            next_end = min(BATCH_SIZE, total)
    
    return jsonify({
        "total_accounts": total,
        "batch_size": BATCH_SIZE,
        "current_batch": batch_num,
        "total_batches": total_batches,
        "current_index": current,
        "current_batch_range": f"{start_idx+1}-{end_idx}",
        "current_batch_size": batch_size,
        "is_partial_batch": batch_size < BATCH_SIZE,
        "total_cycles_completed": cache.total_cycles,
        "next_batch_start": next_start + 1 if next_start < total else 1,
        "loop_status": "Will restart with first 5 accounts" if (end_idx >= total and cache.last_batch_was_partial) else "Normal continuation",
        "progress": f"{current}/{total} accounts used"
    })

# ✅ Home endpoint
@app.route('/')
def home():
    return jsonify({
        "status": "online",
        "message": "Like API with Smart Rotation ✅",
        "features": {
            "auto_refresh": "Every 1 hour",
            "batch_size": f"{BATCH_SIZE} accounts",
            "total_accounts": len(cache.all_accounts),
            "rotation": "Smart - 16+5 logic",
            "cache_type": "In-Memory (Vercel compatible)",
            "total_cycles": cache.total_cycles
        },
        "endpoints": {
            "/like?uid=XXXX": "Send likes to UID",
            "/like?uid=XXXX&refresh=true": "Force refresh tokens",
            "/cache/status": "Check cache status",
            "/cache/refresh": "Refresh token cache",
            "/batch/info": "Get batch information"
        }
    })

# ✅ Initialize cache on startup
def initialize_cache():
    load_accounts()
    print(f"✅ Loaded {len(cache.all_accounts)} total accounts")
    
    # Initial token fetch
    try:
        tokens = asyncio.run(fetch_batch_tokens())
        if tokens:
            total = len(cache.all_accounts)
            current_pos = cache.current_index
            batch_info = {
                'batch_number': 1,
                'total_batches': (total + BATCH_SIZE - 1) // BATCH_SIZE,
                'accounts_used': len(tokens),
                'total_accounts': total,
                'batch_range': f"1-{len(tokens)}",
                'is_partial': len(tokens) < BATCH_SIZE,
                'total_cycles_completed': 0,
                'timestamp': datetime.now().isoformat()
            }
            cache.set_cache(tokens, batch_info)
            print(f"✅ Initial tokens cached: {len(tokens)} tokens")
    except Exception as e:
        print(f"❌ Initial fetch failed: {e}")

# ✅ Initialize on startup
initialize_cache()

# ✅ For local testing
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=3000)
