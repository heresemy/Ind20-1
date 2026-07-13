#GHUMMMMMMM...A
#JOIN CHANNEL  https://t.me/bloodbrx98


from flask import Flask, request, jsonify
import json, os, aiohttp, asyncio, requests, binascii
from datetime import datetime, timedelta
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad
from google.protobuf.json_format import MessageToJson
import like_pb2, like_count_pb2, uid_generator_pb2
from google.protobuf.message import DecodeError

app = Flask(__name__)

ACCOUNTS_FILE = 'accounts.json'
TOKENS_FILE = 'tokens.json'
TOKENS_PER_BATCH = 21
REQUEST_LIMIT = 40
TIME_LIMIT_HOURS = 1

# ✅ Load accounts from accounts.json
def load_accounts():
    if os.path.exists(ACCOUNTS_FILE):
        with open(ACCOUNTS_FILE, 'r') as f:
            data = json.load(f)
            # Handle both list and dict formats
            if isinstance(data, list):
                accounts = {}
                for item in data:
                    if isinstance(item, dict) and 'uid' in item and 'password' in item:
                        accounts[str(item['uid'])] = item['password']
                    elif isinstance(item, list) and len(item) >= 2:
                        accounts[str(item[0])] = item[1]
                return accounts
            elif isinstance(data, dict):
                return data
    return {}

# ✅ Load tokens from tokens.json
def load_tokens():
    if os.path.exists(TOKENS_FILE):
        with open(TOKENS_FILE, 'r') as f:
            return json.load(f)
    return {}

# ✅ Save tokens to tokens.json
def save_tokens(tokens_data):
    with open(TOKENS_FILE, 'w') as f:
        json.dump(tokens_data, f, indent=2)

# ✅ Generate tokens for specific accounts
async def generate_tokens_for_accounts(account_slice):
    tokens = []
    async with aiohttp.ClientSession() as session:
        tasks = []
        for uid, password in account_slice.items():
            tasks.append(fetch_token(session, uid, password))
        results = await asyncio.gather(*tasks)
        
        uids = list(account_slice.keys())
        for i, token in enumerate(results):
            if token:
                tokens.append({
                    'uid': uids[i],
                    'token': token,
                    'created_at': datetime.now().isoformat(),
                    'request_count': 0
                })
    return tokens

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

# ✅ Get next batch of tokens (rotation logic)
def get_next_token_batch(tokens_data, accounts):
    current_time = datetime.now()
    
    # If no accounts, return empty
    if not accounts:
        return []
    
    # If no tokens or empty tokens, generate fresh
    if not tokens_data or not tokens_data.get('tokens'):
        return generate_fresh_tokens(accounts)
    
    # Get current tokens
    current_tokens = tokens_data.get('tokens', [])
    
    # Check if any token needs refresh
    needs_refresh = False
    for token_info in current_tokens:
        created_time = datetime.fromisoformat(token_info['created_at'])
        time_diff = current_time - created_time
        
        # Check time limit or request count
        if time_diff > timedelta(hours=TIME_LIMIT_HOURS) or token_info.get('request_count', 0) >= REQUEST_LIMIT:
            needs_refresh = True
            break
    
    if needs_refresh:
        # Get next batch of accounts (rotation)
        used_accounts = [t['uid'] for t in current_tokens]
        available_accounts = [uid for uid in accounts.keys() if uid not in used_accounts]
        
        # Get accounts for next batch
        if len(available_accounts) >= TOKENS_PER_BATCH:
            # Take next batch
            next_batch = {uid: accounts[uid] for uid in available_accounts[:TOKENS_PER_BATCH]}
        else:
            # Not enough new accounts, start from beginning
            next_batch = {}
            account_list = list(accounts.items())
            
            # Add remaining available accounts
            for uid in available_accounts:
                next_batch[uid] = accounts[uid]
            
            # Add from beginning to complete batch
            remaining = TOKENS_PER_BATCH - len(next_batch)
            for i in range(remaining):
                if i < len(account_list):
                    uid, pwd = account_list[i]
                    next_batch[uid] = pwd
                else:
                    # If still not enough, start from beginning again
                    idx = i % len(account_list)
                    uid, pwd = account_list[idx]
                    next_batch[uid] = pwd
        
        return generate_fresh_tokens(next_batch)
    
    return current_tokens

# ✅ Generate fresh tokens for given accounts
def generate_fresh_tokens(account_slice):
    if not account_slice:
        return []
    
    new_tokens = asyncio.run(generate_tokens_for_accounts(account_slice))
    tokens_data = {
        'tokens': new_tokens,
        'last_updated': datetime.now().isoformat()
    }
    save_tokens(tokens_data)
    return new_tokens

# ✅ Encrypt message
def encrypt_message(plaintext):
    key = b'Yg&tc%DEuh6%Zc^8'
    iv = b'6oyZDr22E3ychjM%'
    cipher = AES.new(key, AES.MODE_CBC, iv)
    return binascii.hexlify(cipher.encrypt(pad(plaintext, AES.block_size))).decode()

# ✅ Create protobufs
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

# ✅ Make request to get player info
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

# ✅ Send like request
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

# ✅ Send likes with token management
async def send_likes_with_token_management(uid, tokens):
    enc_uid = encrypt_message(create_like_proto(uid))
    tasks = []
    
    for token_info in tokens:
        token = token_info['token']
        tasks.append(send_request(enc_uid, token))
    
    results = await asyncio.gather(*tasks)
    
    # Update request count for tokens
    tokens_data = load_tokens()
    if tokens_data and 'tokens' in tokens_data:
        for i, token_info in enumerate(tokens_data['tokens']):
            if i < len(results):
                token_info['request_count'] = token_info.get('request_count', 0) + 1
        save_tokens(tokens_data)
    
    return results

# ✅ Main like endpoint
@app.route('/like', methods=['GET'])
def like_handler():
    uid = request.args.get("uid")
    if not uid:
        return jsonify({"error": "Missing UID"}), 400
    
    try:
        # Load accounts and tokens
        accounts = load_accounts()
        if not accounts:
            return jsonify({"error": "No accounts available"}), 401
        
        tokens_data = load_tokens()
        
        # Get or refresh tokens
        tokens = get_next_token_batch(tokens_data, accounts)
        if not tokens:
            return jsonify({"error": "Failed to generate tokens"}), 401
        
        # Get first token for checking player info
        first_token = tokens[0]['token']
        
        # Get player info before likes
        enc_uid = encrypt_message(create_uid_proto(uid))
        before = make_request(enc_uid, first_token)
        if not before:
            return jsonify({"error": "Failed to retrieve player info"}), 500
        
        before_data = json.loads(MessageToJson(before))
        likes_before = int(before_data.get("AccountInfo", {}).get("Likes", 0))
        nickname = before_data.get("AccountInfo", {}).get("PlayerNickname", "Unknown")
        
        # Send likes
        responses = asyncio.run(send_likes_with_token_management(uid, tokens))
        success_count = sum(1 for r in responses if r == 200)
        
        # Get player info after likes
        after = make_request(enc_uid, first_token)
        likes_after = likes_before
        if after:
            after_data = json.loads(MessageToJson(after))
            likes_after = int(after_data.get("AccountInfo", {}).get("Likes", 0))
        
        return jsonify({
            "PlayerNickname": nickname,
            "UID": uid,
            "LikesBefore": likes_before,
            "LikesAfter": likes_after,
            "LikesGivenByAPI": likes_after - likes_before,
            "SuccessfulRequests": success_count,
            "TotalRequests": len(tokens),
            "TokensUsed": len(tokens),
            "status": 1 if likes_after > likes_before else 2,
            "developer": "semy",
            "token_rotation": "Active"
        })
    
    except Exception as e:
        return jsonify({"error": f"Internal server error: {str(e)}"}), 500

# ✅ Status endpoint
@app.route('/status', methods=['GET'])
def status_handler():
    tokens_data = load_tokens()
    accounts = load_accounts()
    
    response = {
        'total_accounts': len(accounts),
        'tokens_per_batch': TOKENS_PER_BATCH,
        'request_limit': REQUEST_LIMIT,
        'time_limit_hours': TIME_LIMIT_HOURS,
        'token_status': 'No tokens available'
    }
    
    if tokens_data and 'tokens' in tokens_data:
        tokens = tokens_data['tokens']
        token_info = [{
            'uid': t['uid'],
            'requests_used': t.get('request_count', 0),
            'created_at': t['created_at']
        } for t in tokens]
        
        response.update({
            'total_tokens': len(tokens),
            'token_status': 'Active',
            'tokens': token_info,
            'last_updated': tokens_data.get('last_updated')
        })
    
    return jsonify(response)

# ✅ Home endpoint
@app.route('/')
def home():
    return jsonify({
        "status": "online",
        "message": "Like API is running ✅",
        "endpoints": {
            "/like?uid=YOUR_UID": "Send likes to player",
            "/status": "Check token status"
        },
        "token_rotation": f"{TOKENS_PER_BATCH} tokens per batch, refresh after {REQUEST_LIMIT} requests or {TIME_LIMIT_HOURS} hour",
        "total_accounts": len(load_accounts())
    })

# ✅ Local development
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=3000)
