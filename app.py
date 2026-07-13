from flask import Flask, request, jsonify
import json, os, aiohttp, asyncio, requests, binascii, time
from datetime import datetime
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad
from google.protobuf.json_format import MessageToJson
import like_pb2, like_count_pb2, uid_generator_pb2
from google.protobuf.message import DecodeError
from threading import Lock
from collections import defaultdict

app = Flask(__name__)

ACCOUNTS_FILE = 'accounts.json'
BATCH_SIZE = 22  # 🔥 Fixed batch size
MAX_REQUESTS_PER_BATCH = 45  # 🔥 30 requests ke baad next batch
BATCH_EXPIRY_HOURS = 2  # 🔥 2 hours ke baad next batch

# Token Manager
token_manager = {
    'current_batch_tokens': [],      # Current batch ke tokens
    'current_batch_accounts': [],    # Current batch ke account UIDs
    'current_batch_index': 0,        # Current batch number
    'batch_start_index': 0,          # Starting index in accounts list
    'request_count': 0,              # Kitni requests ho chuki hain
    'batch_created_at': None,        # Batch create time
    'total_accounts': 0,
    'all_accounts': [],              # Complete accounts list
    'lock': Lock()
}

# ✅ Load accounts
def load_accounts():
    if os.path.exists(ACCOUNTS_FILE):
        with open(ACCOUNTS_FILE, 'r') as f:
            accounts = json.load(f)
            token_manager['all_accounts'] = list(accounts.items())
            token_manager['total_accounts'] = len(token_manager['all_accounts'])
            print(f"✅ Loaded {token_manager['total_accounts']} accounts")
            return accounts
    return {}

# ✅ Get next batch of accounts (Round-Robin)
def get_next_batch():
    with token_manager['lock']:
        all_accounts = token_manager['all_accounts']
        total = len(all_accounts)
        
        if total == 0:
            return []
        
        start = token_manager['batch_start_index']
        end = start + BATCH_SIZE
        
        # Agar end total se bada hai toh last batch hai
        if end >= total:
            batch = all_accounts[start:total]  # Remaining accounts
            # Next batch start se hoga
            token_manager['batch_start_index'] = 0
        else:
            batch = all_accounts[start:end]
            token_manager['batch_start_index'] = end
        
        print(f"\n📦 BATCH {token_manager['current_batch_index'] + 1}")
        print(f"   Accounts: {len(batch)}")
        print(f"   Range: {start} to {end if end < total else total}")
        print(f"   UIDs: {[uid for uid, _ in batch]}")
        
        return batch

# ✅ Fetch token for single account
async def fetch_token(session, uid, password):
    url = f"https://jwtmc.vercel.app/token?uid={uid}&password={password}"
    try:
        async with session.get(url, timeout=10) as res:
            if res.status == 200:
                text = await res.text()
                try:
                    data = json.loads(text)
                    if isinstance(data, list) and len(data) > 0 and "token" in data[0]:
                        return uid, data[0]["token"], True
                    elif isinstance(data, dict) and "token" in data:
                        return uid, data["token"], True
                except:
                    pass
    except:
        pass
    return uid, None, False

# ✅ Generate tokens for current batch
async def generate_batch_tokens():
    # Get next batch of accounts
    batch_accounts = get_next_batch()
    
    if not batch_accounts:
        print("❌ No accounts available!")
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
                print(f"   ✅ {uid}: Token generated")
            else:
                print(f"   ❌ {uid}: Failed")
    
    # Update token manager
    with token_manager['lock']:
        token_manager['current_batch_tokens'] = tokens
        token_manager['current_batch_accounts'] = token_accounts
        token_manager['current_batch_index'] += 1
        token_manager['request_count'] = 0  # Reset request count
        token_manager['batch_created_at'] = time.time()
    
    print(f"\n✅ Batch complete: {len(tokens)}/{len(batch_accounts)} tokens generated")
    print(f"📊 Total batches so far: {token_manager['current_batch_index']}")
    
    return tokens

# ✅ Check if need new batch
def need_new_batch():
    with token_manager['lock']:
        # Check if we need new batch
        if not token_manager['current_batch_tokens']:
            return True
        
        # Check request count (30 requests)
        if token_manager['request_count'] >= MAX_REQUESTS_PER_BATCH:
            print(f"⏰ Request limit reached: {token_manager['request_count']}/{MAX_REQUESTS_PER_BATCH}")
            return True
        
        # Check time (2 hours)
        if token_manager['batch_created_at']:
            age = time.time() - token_manager['batch_created_at']
            if age >= (BATCH_EXPIRY_HOURS * 3600):
                print(f"⏰ Batch expired: {age/3600:.1f} hours old")
                return True
        
        return False

# ✅ Get tokens (auto-generate if needed)
async def get_tokens(force_refresh=False):
    if force_refresh or need_new_batch():
        print("\n🔄 Generating new batch...")
        return await generate_batch_tokens()
    
    with token_manager['lock']:
        # Increment request count
        token_manager['request_count'] += 1
        print(f"\n📊 Using cached batch - Request {token_manager['request_count']}/{MAX_REQUESTS_PER_BATCH}")
        print(f"   Tokens available: {len(token_manager['current_batch_tokens'])}")
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
    except:
        return None

async def send_likes(uid, force_refresh=False):
    tokens = await get_tokens(force_refresh)
    if not tokens:
        return [], 0
    
    enc_uid = encrypt_message(create_like_proto(uid))
    
    # Send likes using all tokens in current batch
    tasks = [send_request(enc_uid, token) for token in tokens]
    responses = await asyncio.gather(*tasks)
    success_count = sum(1 for r in responses if r == 200)
    
    return responses, success_count

# ✅ Main endpoint
@app.route('/like', methods=['GET'])
def like_handler():
    uid = request.args.get("uid")
    force_refresh = request.args.get("refresh", "false").lower() == "true"
    
    if not uid:
        return jsonify({"error": "Missing UID"}), 400
    
    try:
        # Get tokens (auto-generate if needed)
        tokens = asyncio.run(get_tokens(force_refresh))
        
        if not tokens:
            return jsonify({
                "error": "No valid tokens available",
                "message": "All accounts in current batch failed."
            }), 401
        
        # Get player info
        enc_uid = encrypt_message(create_uid_proto(uid))
        before = make_request(enc_uid, tokens[0])
        
        if not before:
            return jsonify({"error": "Failed to retrieve player info"}), 500
        
        before_data = json.loads(MessageToJson(before))
        likes_before = int(before_data.get("AccountInfo", {}).get("Likes", 0))
        nickname = before_data.get("AccountInfo", {}).get("PlayerNickname", "Unknown")
        level = before_data.get("AccountInfo", {}).get("level", 0)
        
        # Send likes
        responses, success_count = asyncio.run(send_likes(uid, force_refresh))
        
        # Wait for update
        time.sleep(1)
        
        # Get updated info
        after = make_request(enc_uid, tokens[0])
        likes_after = likes_before
        if after:
            after_data = json.loads(MessageToJson(after))
            likes_after = int(after_data.get("AccountInfo", {}).get("Likes", 0))
        
        # Prepare batch info
        with token_manager['lock']:
            batch_info = {
                "CurrentBatch": token_manager['current_batch_index'],
                "BatchSize": len(token_manager['current_batch_tokens']),
                "AccountsInBatch": token_manager['current_batch_accounts'],
                "RequestsUsed": token_manager['request_count'],
                "MaxRequests": MAX_REQUESTS_PER_BATCH,
                "RemainingRequests": MAX_REQUESTS_PER_BATCH - token_manager['request_count'],
                "TotalAccounts": token_manager['total_accounts'],
                "NextBatchWillStart": "After 30 requests or 2 hours"
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

# ✅ Status endpoint
@app.route('/')
def home():
    with token_manager['lock']:
        info = {
            "status": "online",
            "message": "Like API with Auto-Rotating Batch System ✅",
            "config": {
                "batch_size": BATCH_SIZE,
                "max_requests_per_batch": MAX_REQUESTS_PER_BATCH,
                "batch_expiry_hours": BATCH_EXPIRY_HOURS
            },
            "current_status": {
                "total_accounts": token_manager['total_accounts'],
                "current_batch_tokens": len(token_manager['current_batch_tokens']),
                "current_batch_accounts": token_manager['current_batch_accounts'],
                "current_batch_number": token_manager['current_batch_index'],
                "requests_used": token_manager['request_count'],
                "requests_remaining": MAX_REQUESTS_PER_BATCH - token_manager['request_count'],
                "batch_age": f"{(time.time() - token_manager['batch_created_at']) / 3600:.1f} hours" if token_manager['batch_created_at'] else "N/A"
            }
        }
    return jsonify(info)

# ✅ Force next batch
@app.route('/next-batch', methods=['POST'])
def force_next_batch():
    """Force generate next batch of tokens"""
    try:
        # Clear current tokens
        with token_manager['lock']:
            token_manager['current_batch_tokens'] = []
            token_manager['current_batch_accounts'] = []
            token_manager['request_count'] = MAX_REQUESTS_PER_BATCH  # Force new batch
        
        # Generate next batch
        tokens = asyncio.run(generate_batch_tokens())
        
        return jsonify({
            "status": "success",
            "message": f"Next batch generated with {len(tokens)} tokens",
            "batch_number": token_manager['current_batch_index'],
            "accounts": token_manager['current_batch_accounts']
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ✅ Reset to first batch
@app.route('/reset', methods=['POST'])
def reset_batch():
    """Reset to first batch"""
    with token_manager['lock']:
        token_manager['batch_start_index'] = 0
        token_manager['current_batch_index'] = 0
        token_manager['current_batch_tokens'] = []
        token_manager['current_batch_accounts'] = []
        token_manager['request_count'] = MAX_REQUESTS_PER_BATCH
    
    return jsonify({
        "status": "success",
        "message": "Reset to first batch. Next request will generate batch 1."
    })

# ✅ Initialize
load_accounts()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=3000)