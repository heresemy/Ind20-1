from flask import Flask, request, jsonify
import json, os, aiohttp, asyncio, requests, binascii, time
from datetime import datetime
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad
from google.protobuf.json_format import MessageToJson
import like_pb2, like_count_pb2, uid_generator_pb2
from google.protobuf.message import DecodeError
from threading import Lock

app = Flask(__name__)

ACCOUNTS_FILE = 'accounts.json'
BATCH_SIZE = 20
MAX_REQUESTS_PER_BATCH = 30
BATCH_EXPIRY_HOURS = 2

token_manager = {
    'current_batch_tokens': [],
    'current_batch_accounts': [],
    'current_batch_index': 0,
    'batch_start_index': 0,
    'request_count': 0,
    'batch_created_at': None,
    'total_accounts': 0,
    'all_accounts': [],
    'lock': Lock(),
    'is_generating': False  # 🔥 Generation lock
}

# ✅ Load accounts from file
def load_accounts():
    try:
        if os.path.exists(ACCOUNTS_FILE):
            with open(ACCOUNTS_FILE, 'r', encoding='utf-8') as f:
                accounts = json.load(f)
                token_manager['all_accounts'] = list(accounts.items())
                token_manager['total_accounts'] = len(token_manager['all_accounts'])
                print(f"✅ Loaded {len(accounts)} accounts from {ACCOUNTS_FILE}")
                return accounts
        else:
            print(f"❌ {ACCOUNTS_FILE} not found!")
            return {}
    except Exception as e:
        print(f"❌ Error loading accounts: {e}")
        return {}

# ✅ Validate token (check if token is valid)
async def validate_token(token):
    """Check if token is valid by making a test request"""
    try:
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
            'ReleaseVersion': "OB53"
        }
        
        # Test with a dummy UID (123456789)
        enc_uid = encrypt_message(create_uid_proto("123456789"))
        
        async with aiohttp.ClientSession() as session:
            async with session.post(url, data=bytes.fromhex(enc_uid), headers=headers, ssl=False, timeout=10) as res:
                if res.status == 200:
                    return True
                else:
                    return False
    except:
        return False

# ✅ Get next batch (Round-Robin)
def get_next_batch():
    with token_manager['lock']:
        all_accounts = token_manager['all_accounts']
        total = len(all_accounts)
        
        if total == 0:
            return []
        
        start = token_manager['batch_start_index']
        end = start + BATCH_SIZE
        
        if end >= total:
            batch = all_accounts[start:total]
            token_manager['batch_start_index'] = 0
        else:
            batch = all_accounts[start:end]
            token_manager['batch_start_index'] = end
        
        return batch

# ✅ Fetch token
async def fetch_token(session, uid, password):
    url = f"https://jwtmc.vercel.app/token?uid={uid}&password={password}"
    try:
        async with session.get(url, timeout=10) as res:
            if res.status == 200:
                text = await res.text()
                try:
                    data = json.loads(text)
                    if isinstance(data, list) and len(data) > 0 and "token" in data[0]:
                        token = data[0]["token"]
                        # ✅ Validate token immediately
                        if await validate_token(token):
                            return uid, token, True
                        else:
                            return uid, None, False
                    elif isinstance(data, dict) and "token" in data:
                        token = data["token"]
                        if await validate_token(token):
                            return uid, token, True
                        else:
                            return uid, None, False
                except:
                    pass
    except:
        pass
    return uid, None, False

# ✅ Generate batch tokens with auto-retry
async def generate_batch_tokens(force=False):
    with token_manager['lock']:
        if token_manager['is_generating'] and not force:
            print("⏳ Token generation already in progress...")
            return token_manager['current_batch_tokens'].copy()
        token_manager['is_generating'] = True
    
    try:
        batch_accounts = get_next_batch()
        
        if not batch_accounts:
            with token_manager['lock']:
                token_manager['is_generating'] = False
            return []
        
        print(f"\n🔄 Generating tokens for {len(batch_accounts)} accounts...")
        
        tokens = []
        token_accounts = []
        
        async with aiohttp.ClientSession() as session:
            tasks = [fetch_token(session, uid, pwd) for uid, pwd in batch_accounts]
            results = await asyncio.gather(*tasks)
            
            for uid, token, success in results:
                if success and token:
                    tokens.append(token)
                    token_accounts.append(uid)
                    print(f"   ✅ {uid}: Token generated & validated")
                else:
                    print(f"   ❌ {uid}: Failed or invalid token")
        
        # If no tokens generated, try next batch
        if not tokens:
            print("⚠️ No valid tokens in this batch, trying next batch...")
            # Move to next batch
            with token_manager['lock']:
                token_manager['batch_start_index'] = (token_manager['batch_start_index'] + BATCH_SIZE) % token_manager['total_accounts']
            
            # Retry with next batch
            return await generate_batch_tokens(force=True)
        
        with token_manager['lock']:
            token_manager['current_batch_tokens'] = tokens
            token_manager['current_batch_accounts'] = token_accounts
            token_manager['current_batch_index'] += 1
            token_manager['request_count'] = 0
            token_manager['batch_created_at'] = time.time()
            token_manager['is_generating'] = False
        
        print(f"\n✅ Batch complete: {len(tokens)}/{len(batch_accounts)} valid tokens")
        return tokens
    
    except Exception as e:
        print(f"❌ Error generating batch: {e}")
        with token_manager['lock']:
            token_manager['is_generating'] = False
        return []

# ✅ Check if need new batch
def need_new_batch():
    with token_manager['lock']:
        # Check if no tokens
        if not token_manager['current_batch_tokens']:
            print("⚠️ No tokens available, need new batch")
            return True
        
        # Check request count
        if token_manager['request_count'] >= MAX_REQUESTS_PER_BATCH:
            print(f"⏰ Request limit reached: {token_manager['request_count']}/{MAX_REQUESTS_PER_BATCH}")
            return True
        
        # Check time expiry
        if token_manager['batch_created_at']:
            age = time.time() - token_manager['batch_created_at']
            if age >= (BATCH_EXPIRY_HOURS * 3600):
                print(f"⏰ Batch expired: {age/3600:.1f} hours old")
                return True
        
        return False

# ✅ Get tokens with auto-generation
async def get_tokens(force_refresh=False):
    # If force refresh, generate new batch
    if force_refresh:
        print("🔄 Force refresh requested")
        return await generate_batch_tokens(force=True)
    
    # Check if need new batch
    if need_new_batch():
        return await generate_batch_tokens()
    
    # Return cached tokens
    with token_manager['lock']:
        token_manager['request_count'] += 1
        print(f"\n📊 Using cached batch - Request {token_manager['request_count']}/{MAX_REQUESTS_PER_BATCH}")
        return token_manager['current_batch_tokens'].copy()

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
        'ReleaseVersion': "OB53"
    }
    try:
        res = requests.post(url, data=bytes.fromhex(enc_uid), headers=headers, verify=False, timeout=10)
        if res.status == 200:
            return decode_protobuf(res.content)
        return None
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
        'ReleaseVersion': "OB53"
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, data=bytes.fromhex(enc_uid), headers=headers, ssl=False, timeout=10) as r:
                return r.status
    except:
        return None

async def send_likes(uid, force_refresh=False):
    tokens = await get_tokens(force_refresh)
    if not tokens:
        return [], 0
    
    enc_uid = encrypt_message(create_like_proto(uid))
    
    # Max 10 tokens per request (Vercel timeout)
    max_tokens = min(len(tokens), 10)
    
    tasks = [send_request(enc_uid, token) for token in tokens[:max_tokens]]
    responses = await asyncio.gather(*tasks)
    success_count = sum(1 for r in responses if r == 200)
    
    # If all requests failed, tokens might be invalid
    if success_count == 0 and len(tokens) > 0:
        print("⚠️ All like requests failed, tokens might be invalid")
        # Force refresh for next request
        with token_manager['lock']:
            token_manager['current_batch_tokens'] = []
    
    return responses, success_count

# ✅ Main endpoint
@app.route('/like', methods=['GET'])
def like_handler():
    uid = request.args.get("uid")
    force_refresh = request.args.get("refresh", "false").lower() == "true"
    
    if not uid:
        return jsonify({"error": "Missing UID"}), 400
    
    try:
        # Get tokens with auto-generation
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        tokens = loop.run_until_complete(get_tokens(force_refresh))
        loop.close()
        
        if not tokens:
            return jsonify({
                "error": "No valid tokens available",
                "message": "All accounts failed. Auto-retry enabled.",
                "action": "Try again in a few seconds"
            }), 401
        
        # Get player info
        enc_uid = encrypt_message(create_uid_proto(uid))
        before = make_request(enc_uid, tokens[0])
        
        if not before:
            return jsonify({
                "error": "Failed to retrieve player info",
                "message": "Token might be invalid, auto-refreshing..."
            }), 500
        
        before_data = json.loads(MessageToJson(before))
        likes_before = int(before_data.get("AccountInfo", {}).get("Likes", 0))
        nickname = before_data.get("AccountInfo", {}).get("PlayerNickname", "Unknown")
        level = before_data.get("AccountInfo", {}).get("level", 0)
        
        # Send likes
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        responses, success_count = loop.run_until_complete(send_likes(uid, force_refresh))
        loop.close()
        
        # Get updated info
        after = make_request(enc_uid, tokens[0])
        likes_after = likes_before
        if after:
            after_data = json.loads(MessageToJson(after))
            likes_after = int(after_data.get("AccountInfo", {}).get("Likes", 0))
        
        with token_manager['lock']:
            batch_info = {
                "CurrentBatch": token_manager['current_batch_index'],
                "BatchSize": len(token_manager['current_batch_tokens']),
                "RequestsUsed": token_manager['request_count'],
                "MaxRequests": MAX_REQUESTS_PER_BATCH,
                "RemainingRequests": MAX_REQUESTS_PER_BATCH - token_manager['request_count'],
                "TotalAccounts": token_manager['total_accounts'],
                "AutoRefresh": "Enabled"
            }
        
        return jsonify({
            "PlayerNickname": nickname,
            "UID": uid,
            "Level": level,
            "LikesBefore": likes_before,
            "LikesAfter": likes_after,
            "LikesGivenByAPI": likes_after - likes_before,
            "SuccessfulRequests": success_count,
            "TotalRequests": len(tokens),
            "BatchInfo": batch_info,
            "status": 1 if likes_after > likes_before else 2,
            "developer": "semy"
        })
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/')
def home():
    with token_manager['lock']:
        info = {
            "status": "online",
            "message": "Like API with Auto-Token Generation ✅",
            "features": {
                "auto_token_generation": "Enabled",
                "token_validation": "Enabled",
                "auto_retry": "Enabled",
                "batch_rotation": "Round-Robin"
            },
            "config": {
                "batch_size": BATCH_SIZE,
                "max_requests_per_batch": MAX_REQUESTS_PER_BATCH,
                "batch_expiry_hours": BATCH_EXPIRY_HOURS
            },
            "current_status": {
                "total_accounts": token_manager['total_accounts'],
                "current_batch_tokens": len(token_manager['current_batch_tokens']),
                "current_batch_number": token_manager['current_batch_index'],
                "requests_used": token_manager['request_count'],
                "requests_remaining": MAX_REQUESTS_PER_BATCH - token_manager['request_count'],
                "is_generating": token_manager['is_generating']
            }
        }
    return jsonify(info)

@app.route('/force-generate', methods=['POST'])
def force_generate():
    """Force generate new tokens"""
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        tokens = loop.run_until_complete(generate_batch_tokens(force=True))
        loop.close()
        
        return jsonify({
            "status": "success",
            "message": f"Generated {len(tokens)} tokens",
            "tokens": len(tokens)
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ✅ Load accounts on startup
load_accounts()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=3000)
