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
TOKENS_PER_BATCH = 21  # ✅ Sirf 21 tokens per hit
REQUEST_LIMIT = 40
TIME_LIMIT_HOURS = 1

# ✅ Load accounts from accounts.json (supports both formats)
def load_accounts():
    if os.path.exists(ACCOUNTS_FILE):
        with open(ACCOUNTS_FILE, 'r') as f:
            data = json.load(f)
            
            # If data is a list of objects with uid/password
            if isinstance(data, list):
                accounts = {}
                for item in data:
                    if isinstance(item, dict):
                        # Try to get uid and password from various possible keys
                        uid = item.get('uid') or item.get('account_id') or item.get('id')
                        password = item.get('password') or item.get('pass') or item.get('token')
                        
                        if uid and password:
                            accounts[str(uid)] = str(password)
                    elif isinstance(item, list) and len(item) >= 2:
                        accounts[str(item[0])] = str(item[1])
                return accounts
            
            # If data is a dict directly
            elif isinstance(data, dict):
                return {str(k): str(v) for k, v in data.items()}
    
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

# ✅ Fetch token from API - Updated for new response format
async def fetch_token(session, uid, password):
    url = f"https://railjwt-production.up.railway.app/semy?uid={uid}&password={password}"
    try:
        async with session.get(url, timeout=15) as res:
            if res.status == 200:
                text = await res.text()
                try:
                    data = json.loads(text)
                    
                    # Check new API response format
                    if data.get('success') == True:
                        # Direct jwt field
                        if 'jwt' in data:
                            return data['jwt']
                        
                        # Nested jwt field
                        if 'data' in data and isinstance(data['data'], dict):
                            if 'jwt' in data['data']:
                                return data['data']['jwt']
                    
                    # Fallback: Check for token in any format
                    if isinstance(data, dict):
                        # Check common token field names
                        for key in ['jwt', 'token', 'Token', 'access_token', 'accessToken']:
                            if key in data:
                                return data[key]
                        
                        # Check nested data
                        if 'data' in data and isinstance(data['data'], dict):
                            for key in ['jwt', 'token', 'Token', 'access_token', 'accessToken']:
                                if key in data['data']:
                                    return data['data'][key]
                    
                    # Handle list response format
                    if isinstance(data, list) and len(data) > 0:
                        if isinstance(data[0], dict):
                            for key in ['jwt', 'token', 'Token']:
                                if key in data[0]:
                                    return data[0][key]
                    
                    return None
                    
                except json.JSONDecodeError:
                    # If response is plain text, might be token directly
                    if len(text) > 50:
                        return text
                    return None
            else:
                print(f"❌ API Error for UID {uid}: Status {res.status}")
                return None
                
    except asyncio.TimeoutError:
        print(f"⏰ Timeout for UID {uid}")
        return None
    except Exception as e:
        print(f"❌ Error fetching token for UID {uid}: {str(e)}")
        return None

# ✅ Generate tokens for specific accounts
async def generate_tokens_for_accounts(account_slice):
    tokens = []
    async with aiohttp.ClientSession() as session:
        tasks = []
        uids = []
        for uid, password in account_slice.items():
            tasks.append(fetch_token(session, uid, password))
            uids.append(uid)
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                print(f"❌ Error for UID {uids[i]}: {str(result)}")
                continue
            if result:
                tokens.append({
                    'uid': uids[i],
                    'token': result,
                    'created_at': datetime.now().isoformat(),
                    'request_count': 0
                })
                print(f"✅ Token generated for UID: {uids[i]}")
            else:
                print(f"❌ Failed to generate token for UID: {uids[i]}")
    
    return tokens

# ✅ Get next batch of tokens (rotation logic) - FIXED
def get_next_token_batch(tokens_data, accounts):
    current_time = datetime.now()
    
    if not accounts:
        print("❌ No accounts available")
        return []
    
    # If no tokens or empty tokens, generate fresh with TOKENS_PER_BATCH only
    if not tokens_data or not tokens_data.get('tokens'):
        print(f"🔄 Generating fresh {TOKENS_PER_BATCH} tokens...")
        # ✅ Take only first TOKENS_PER_BATCH accounts
        account_slice = {}
        account_list = list(accounts.items())
        for i in range(min(TOKENS_PER_BATCH, len(account_list))):
            uid, pwd = account_list[i]
            account_slice[uid] = pwd
        return generate_fresh_tokens(account_slice)
    
    # Get current tokens
    current_tokens = tokens_data.get('tokens', [])
    
    # Check if any token needs refresh (only check active batch)
    needs_refresh = False
    for token_info in current_tokens[:TOKENS_PER_BATCH]:
        try:
            created_time = datetime.fromisoformat(token_info['created_at'])
            time_diff = current_time - created_time
            
            if time_diff > timedelta(hours=TIME_LIMIT_HOURS) or token_info.get('request_count', 0) >= REQUEST_LIMIT:
                needs_refresh = True
                print(f"🔄 Token refresh needed for UID {token_info['uid']} (Requests: {token_info.get('request_count', 0)})")
                break
        except:
            needs_refresh = True
            break
    
    if needs_refresh:
        # ✅ Get next batch of exactly TOKENS_PER_BATCH accounts
        used_accounts = [t['uid'] for t in current_tokens]
        available_accounts = [uid for uid in accounts.keys() if uid not in used_accounts]
        
        print(f"📊 Used accounts: {len(used_accounts)}, Available: {len(available_accounts)}")
        
        next_batch = {}
        account_list = list(accounts.items())
        
        # Take from available accounts
        for uid in available_accounts[:TOKENS_PER_BATCH]:
            next_batch[uid] = accounts[uid]
        
        # Fill remaining from beginning if needed
        if len(next_batch) < TOKENS_PER_BATCH:
            remaining = TOKENS_PER_BATCH - len(next_batch)
            for i in range(remaining):
                if i < len(account_list):
                    uid, pwd = account_list[i]
                    if uid not in next_batch:
                        next_batch[uid] = pwd
                else:
                    idx = i % len(account_list)
                    uid, pwd = account_list[idx]
                    if uid not in next_batch:
                        next_batch[uid] = pwd
        
        print(f"🔄 New batch: {len(next_batch)} accounts")
        return generate_fresh_tokens(next_batch)
    
    # ✅ Return only TOKENS_PER_BATCH tokens
    return current_tokens[:TOKENS_PER_BATCH]

# ✅ Generate fresh tokens for given accounts
def generate_fresh_tokens(account_slice):
    if not account_slice:
        print("❌ No accounts in slice")
        return []
    
    print(f"🔄 Generating tokens for {len(account_slice)} accounts...")
    new_tokens = asyncio.run(generate_tokens_for_accounts(account_slice))
    
    if not new_tokens:
        print("❌ No tokens generated!")
        return []
    
    tokens_data = {
        'tokens': new_tokens,
        'last_updated': datetime.now().isoformat()
    }
    save_tokens(tokens_data)
    print(f"✅ Saved {len(new_tokens)} tokens to tokens.json")
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
        res = requests.post(url, data=bytes.fromhex(enc_uid), headers=headers, verify=False, timeout=10)
        return decode_protobuf(res.content)
    except Exception as e:
        print(f"❌ Make request error: {e}")
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
            async with session.post(url, data=bytes.fromhex(enc_uid), headers=headers, ssl=False, timeout=10) as r:
                return r.status
    except Exception as e:
        print(f"❌ Send request error: {e}")
        return None

# ✅ Send likes with token management
async def send_likes_with_token_management(uid, tokens):
    enc_uid = encrypt_message(create_like_proto(uid))
    tasks = []
    
    for token_info in tokens:
        token = token_info['token']
        tasks.append(send_request(enc_uid, token))
    
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Update request count for tokens
    tokens_data = load_tokens()
    if tokens_data and 'tokens' in tokens_data:
        for i, token_info in enumerate(tokens_data['tokens']):
            if i < len(results) and isinstance(results[i], int):
                token_info['request_count'] = token_info.get('request_count', 0) + 1
        save_tokens(tokens_data)
    
    # Count successful requests
    success_count = sum(1 for r in results if r == 200)
    return results, success_count

# ✅ Main like endpoint
@app.route('/like', methods=['GET'])
def like_handler():
    uid = request.args.get("uid")
    if not uid:
        return jsonify({"error": "Missing UID"}), 400
    
    try:
        print(f"📥 Received request for UID: {uid}")
        
        # Load accounts and tokens
        accounts = load_accounts()
        if not accounts:
            print("❌ No accounts available")
            return jsonify({"error": "No accounts available"}), 401
        
        print(f"📊 Total accounts: {len(accounts)}")
        tokens_data = load_tokens()
        
        # Get or refresh tokens
        tokens = get_next_token_batch(tokens_data, accounts)
        if not tokens:
            print("❌ Failed to generate tokens")
            return jsonify({"error": "Failed to generate tokens"}), 401
        
        print(f"✅ Using {len(tokens)} tokens")
        
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
        
        print(f"👤 Player: {nickname}, Likes before: {likes_before}")
        
        # Send likes
        responses, success_count = asyncio.run(send_likes_with_token_management(uid, tokens))
        
        print(f"✅ Success rate: {success_count}/{len(tokens)}")
        
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
        print(f"❌ Error: {str(e)}")
        return jsonify({"error": f"Internal server error: {str(e)}"}), 500

# ✅ Status endpoint
@app.route('/status', methods=['GET'])
def status_handler():
    tokens_data = load_tokens()
    accounts = load_accounts()
    
    # Show sample accounts (first 5)
    sample_accounts = list(accounts.keys())[:5] if accounts else []
    
    response = {
        'total_accounts': len(accounts),
        'sample_accounts': sample_accounts,
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
        } for t in tokens[:TOKENS_PER_BATCH]]  # ✅ Show only active batch
        
        response.update({
            'total_tokens': len(tokens),
            'active_tokens': min(TOKENS_PER_BATCH, len(tokens)),
            'token_status': 'Active',
            'tokens': token_info,
            'last_updated': tokens_data.get('last_updated')
        })
    
    return jsonify(response)

# ✅ Test token endpoint
@app.route('/test/<uid>', methods=['GET'])
def test_token(uid):
    accounts = load_accounts()
    if uid not in accounts:
        return jsonify({"error": "UID not found in accounts"}), 404
    
    password = accounts[uid]
    
    async def test():
        async with aiohttp.ClientSession() as session:
            token = await fetch_token(session, uid, password)
            return token
    
    token = asyncio.run(test())
    if token:
        return jsonify({
            "success": True,
            "uid": uid,
            "token": token[:50] + "...",  # Show partial token
            "full_token": token
        })
    else:
        return jsonify({
            "success": False,
            "uid": uid,
            "message": "Failed to get token"
        }), 400

# ✅ Debug endpoint - Check single like
@app.route('/debug/like', methods=['GET'])
def debug_like():
    uid = request.args.get("uid")
    if not uid:
        return jsonify({"error": "Missing UID"}), 400
    
    try:
        accounts = load_accounts()
        tokens_data = load_tokens()
        
        if not tokens_data or 'tokens' not in tokens_data:
            return jsonify({"error": "No tokens available"}), 401
        
        # Use first token only
        token = tokens_data['tokens'][0]['token']
        
        # Test encryption
        enc_uid = encrypt_message(create_like_proto(uid))
        
        # Send single like
        status = asyncio.run(send_request(enc_uid, token))
        
        # Get player info
        enc_uid_info = encrypt_message(create_uid_proto(uid))
        before = make_request(enc_uid_info, token)
        
        if before:
            before_data = json.loads(MessageToJson(before))
            likes = int(before_data.get("AccountInfo", {}).get("Likes", 0))
            nickname = before_data.get("AccountInfo", {}).get("PlayerNickname", "Unknown")
            
            return jsonify({
                "target_uid": uid,
                "nickname": nickname,
                "current_likes": likes,
                "like_status": status,
                "token_used": token[:30] + "...",
                "message": "Success" if status == 200 else f"Failed with status: {status}"
            })
        
        return jsonify({"error": "Failed to get player info"}), 500
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ✅ Check token validity
@app.route('/debug/token/<uid>', methods=['GET'])
def debug_token(uid):
    accounts = load_accounts()
    if uid not in accounts:
        return jsonify({"error": "UID not found"}), 404
    
    password = accounts[uid]
    
    async def check():
        async with aiohttp.ClientSession() as session:
            # Get token
            token = await fetch_token(session, uid, password)
            if not token:
                return {"error": "Failed to get token"}
            
            # Test token with player info (using a test UID)
            test_uid = "1818702159"  # Change this to any UID
            enc_uid = encrypt_message(create_uid_proto(test_uid))
            result = make_request(enc_uid, token)
            
            if result:
                data = json.loads(MessageToJson(result))
                return {
                    "token_valid": True,
                    "player_info": {
                        "nickname": data.get("AccountInfo", {}).get("PlayerNickname"),
                        "likes": data.get("AccountInfo", {}).get("Likes"),
                        "level": data.get("AccountInfo", {}).get("Level")
                    }
                }
            else:
                return {"token_valid": False, "error": "Token failed to get player info"}
    
    result = asyncio.run(check())
    return jsonify(result)

# ✅ Home endpoint
@app.route('/')
def home():
    accounts = load_accounts()
    return jsonify({
        "status": "online",
        "message": "Like API is running ✅",
        "endpoints": {
            "/like?uid=YOUR_UID": "Send likes to player (21 likes per hit)",
            "/status": "Check token status",
            "/test/UID": "Test token generation for specific UID",
            "/debug/like?uid=UID": "Send single like for testing",
            "/debug/token/UID": "Check token validity"
        },
        "token_rotation": f"{TOKENS_PER_BATCH} tokens per batch, refresh after {REQUEST_LIMIT} requests or {TIME_LIMIT_HOURS} hour",
        "total_accounts": len(accounts),
        "sample_accounts": list(accounts.keys())[:3] if accounts else [],
        "jwt_api": "https://railjwt-production.up.railway.app/semy"
    })

# ✅ Local development
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=3000, debug=True)
