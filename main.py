from fastapi import FastAPI, Depends, Request, Form, Response, HTTPException, status, Query, Body, WebSocket, \
    WebSocketDisconnect
from fastapi.encoders import jsonable_encoder
from fastapi.responses import RedirectResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from starlette.middleware.sessions import SessionMiddleware
from fastapi.staticfiles import StaticFiles
from sqlalchemy import text
from pandas import DataFrame
from typing import Optional, List
from datetime import datetime
import dotenv
import os
import random
import json
import httpx
import websockets
import pybithumb
import base64
import hashlib
import hmac
import time
import urllib.parse
import jwt
import uuid


dotenv.load_dotenv()
DATABASE_URL = os.getenv("dburl")
engine = create_async_engine(DATABASE_URL, echo=True)
async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

app = FastAPI()

app.add_middleware(SessionMiddleware, secret_key="supersecretkey")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
templates = Jinja2Templates(directory="templates")
app.mount("/static", StaticFiles(directory="static"), name="static")


def format_currency(value):
    if isinstance(value, (int, float)):
        return "{:,.0f}".format(value)
    return value


templates.env.filters['currency'] = format_currency


async def get_db():
    async with async_session() as session:
        yield session


async def bithumb_api2_request(method: str, endpoint: str, access_key: str, secret_key: str,
                               params: dict = None) -> list | dict:
    """빗썸 API 2.0 (JWT Bearer) 비동기 호출 헬퍼"""
    url = f"https://api.bithumb.com{endpoint}"
    access_key = access_key.strip()
    secret_key = secret_key.strip()

    payload = {
        'access_key': access_key,
        'nonce': str(uuid.uuid4()),
        'timestamp': int(time.time() * 1000)
    }

    # Query string이나 Body 파라미터가 있을 경우 SHA512 해시 생성
    if params:
        query_string = urllib.parse.urlencode(params)
        m = hashlib.sha512()
        m.update(query_string.encode('utf-8'))
        query_hash = m.hexdigest()
        payload['query_hash'] = query_hash
        payload['query_hash_alg'] = 'SHA512'

    # HS256 알고리즘으로 서명 토큰 생성
    jwt_token = jwt.encode(payload, secret_key, algorithm='HS256')
    headers = {
        'Authorization': f'Bearer {jwt_token}',
        'Content-Type': 'application/json'
    }

    async with httpx.AsyncClient(timeout=5.0) as client:
        if method.upper() == "GET":
            res = await client.get(url, headers=headers, params=params)
        elif method.upper() == "POST":
            res = await client.post(url, headers=headers, json=params)
        elif method.upper() == "DELETE":
            res = await client.delete(url, headers=headers, params=params)
        else:
            raise ValueError(f"Unsupported method: {method}")

        return res.json()


# ==========================================
# 빗썸 전용 시세 조회 유틸리티
# ==========================================

async def get_current_prices():
    """빗썸 원화 마켓 전체 Ticker 조회"""
    url = "https://api.bithumb.com/public/ticker/ALL_KRW"
    async with httpx.AsyncClient() as client:
        res = await client.get(url)
        data = res.json()
        result = []
        if data.get("status") == "0000":
            for coin, ticker in data.get("data", {}).items():
                if coin == "date":
                    continue
                trade_price = ticker.get("closing_price")
                if trade_price:
                    result.append({"market": f"KRW-{coin}", "trade_price": float(trade_price)})
        return result


async def get_current_price(coink: str):
    """단일 코인 현재가 조회 (coink: 'BTC' 또는 'KRW-BTC')"""
    symbol = coink.replace("KRW-", "")
    url = f"https://api.bithumb.com/public/ticker/{symbol}_KRW"
    async with httpx.AsyncClient() as client:
        res = await client.get(url)
        data = res.json()
        if data.get("status") == "0000":
            return float(data["data"].get("closing_price", 0))
        return None


# ==========================================
# DB 및 인증 관련 함수
# ==========================================

async def selectUsers(uid: str, upw: str, db: AsyncSession = Depends(get_db)):
    row = None
    setkey = None
    try:
        sql = text(
            "SELECT userNo, userName, serverNo, userRole, tradeCnt FROM traceUser WHERE userPasswd=password(:passwd) AND userId=:userid AND attrib NOT LIKE :xattr")
        result = await db.execute(sql, {"passwd": upw, "userid": uid, "xattr": "%XXXUP%"})
        row = result.fetchone()
        if row is not None:
            setkey = random.randint(100000, 999999)
    except Exception as e:
        print('접속오류', e)
    finally:
        return row, setkey


async def listUsers(db: AsyncSession = Depends(get_db)):
    rows = None
    try:
        sql = text("SELECT * FROM traceUser WHERE attrib NOT LIKE :xattr")
        result = await db.execute(sql, {"xattr": "%XXXUP%"})
        rows = result.fetchall()
    except Exception as e:
        print('접속오류', e)
    finally:
        return rows


async def get_hotcoins(request, db):
    try:
        query = text("SELECT * FROM orderbookAmt where dateTag = (select max(dateTag) from orderbookAmt)")
        result = await db.execute(query)
        return result.fetchall()
    except Exception as e:
        print("Error!!", e)
        return False


async def get_hotamt(request, db):
    try:
        query = text("select * from tradeAmt order by regDate desc limit 1")
        result = await db.execute(query)
        return result.fetchone()
    except Exception as e:
        print("Error!!", e)
        return False


async def detailuser(uno: int, db: AsyncSession = Depends(get_db)):
    row = None
    try:
        sql = text("SELECT * FROM traceUser WHERE userNo = :userno and attrib NOT LIKE :xattr")
        result = await db.execute(sql, {"userno": uno, "xattr": "%XXXUP%"})
        row = result.fetchone()
    except Exception as e:
        print('접속오류', e)
    finally:
        return row


async def get_onoff(uno: int, db: AsyncSession = Depends(get_db)):
    row = None
    try:
        sql = text("SELECT activeYN FROM mtSetup WHERE userNo = :userno and attrib NOT LIKE :xattr")
        result = await db.execute(sql, {"userno": uno, "xattr": "%XXX%"})
        row = result.fetchone()
    except Exception as e:
        print('접속오류', e)
    finally:
        return row


async def setKeys(uno: int, setkey: str, db: AsyncSession = Depends(get_db)):
    try:
        sql = text("UPDATE traceUser SET setupKey = :setk, lastLogin = now() where userNo=:userno")
        await db.execute(sql, {"userno": uno, "setk": setkey})
        await db.commit()
    except Exception as e:
        print('접속오류', e)
    finally:
        return True


async def check_setkey(uno: int, setkey: str, db: AsyncSession = Depends(get_db)):
    try:
        sql = text("SELECT userNo FROM traceUser WHERE userNo=:userno and setupKey=:setkey and attrib not like :xattr")
        result = await db.execute(sql, {"userno": uno, "setkey": setkey, "xattr": "%XXXUP%"})
        row = result.fetchone()
        return (row is not None and row[0] == uno)
    except Exception as e:
        print("코드 체크 에러", e)
        return False


async def get_userdetail(uno: int, setkey: str, db: AsyncSession = Depends(get_db)):
    try:
        sql = text("SELECT * FROM traceUser WHERE userNo=:userno and setupKey=:setkey and attrib not like :xattr")
        result = await db.execute(sql, {"userno": uno, "setkey": setkey, "xattr": "%XXXUP%"})
        row = result.fetchone()
        return row if (row and row[0] == uno) else False
    except Exception as e:
        print("유저정보 취득 에러", e)
        return False


def require_login(request: Request):
    user_no = request.session.get("user_No")
    if not user_no:
        raise HTTPException(
            status_code=status.HTTP_303_SEE_OTHER,
            headers={"Location": "/"},
            detail="세션이 만료되어 재로그인이 필요합니다."
        )
    return user_no


async def getKeys(uno: int, setkey: str, db: AsyncSession = Depends(get_db)):
    try:
        sql = text(
            "SELECT apiKey1, apiKey2 FROM traceUser WHERE setupKey=:setk AND userNo=:userno and attrib not like :xattr")
        result = await db.execute(sql, {"setk": setkey, "userno": uno, "xattr": "%XXXUP%"})
        keys = result.fetchone()
        if not keys:
            return None, None
        return keys[0], keys[1]
    except Exception as e:
        print('키로드 오류', e)
        return None, None


async def api_getKeys(uno: int, db: AsyncSession = Depends(get_db)):
    try:
        sql = text("SELECT apiKey1, apiKey2 FROM traceUser WHERE userNo=:userno and attrib not like :xattr")
        result = await db.execute(sql, {"userno": uno, "xattr": "%XXXUP%"})
        keys = result.fetchone()
        if not keys:
            return None, None
        return keys[0], keys[1]
    except Exception as e:
        print('키로드 오류', e)
        return None, None


# ==========================================
# 빗썸 계좌 및 주문 처리
# ==========================================

async def bithumb_private_request(endpoint: str, api_key: str, secret_key: str, params: dict = None) -> dict:
    """빗썸 Private API HMAC-SHA512 서명 및 비동기 요청 헬퍼"""
    if params is None:
        params = {}

    url = f"https://api.bithumb.com{endpoint}"
    # 빗썸 규격: 13자리 밀리초 타임스탬프
    nonce = str(int(time.time() * 1000))
    str_data = urllib.parse.urlencode(params)

    # Signature Message: endpoint + chr(0) + urlencode(params) + chr(0) + nonce
    signature_raw = f"{endpoint}\x00{str_data}\x00{nonce}".encode("utf-8")

    hmac_key = secret_key.encode("utf-8")
    signature_hex = hmac.new(hmac_key, signature_raw, hashlib.sha512).hexdigest().encode("utf-8")
    api_sign = base64.b64encode(signature_hex).decode("utf-8")

    headers = {
        "Api-Key": api_key,
        "Api-Sign": api_sign,
        "Api-Nonce": nonce,
        "Content-Type": "application/x-www-form-urlencoded"
    }

    async with httpx.AsyncClient(timeout=5.0) as client:
        res = await client.post(url, headers=headers, data=params)
        return res.json()


async def checkwallet(uno: int, setkey: str, db: AsyncSession = Depends(get_db)):
    try:
        key1, key2 = await getKeys(uno, setkey, db)
        if not key1 or not key2:
            return []

        # 빗썸 API 2.0 전체 잔고 조회 엔드포인트
        accounts = await bithumb_api2_request("GET", "/v1/accounts", key1, key2)

        # 에러 응답인 경우 (예: {"error": {"message": ...}})
        if isinstance(accounts, dict) and "error" in accounts:
            print("빗썸 API 2.0 지갑 조회 실패:", accounts)
            return []

        return accounts
    except Exception as e:
        print("지갑 불러오기 에러:", e)
        return []


async def api_checkwallet(uno: int, db: AsyncSession = Depends(get_db)):
    try:
        key1, key2 = await api_getKeys(uno, db)
        if not key1 or not key2:
            return []

        accounts = await bithumb_api2_request("GET", "/v1/accounts", key1, key2)
        if isinstance(accounts, dict) and "error" in accounts:
            print("빗썸 API 2.0 지갑 조회 실패:", accounts)
            return []

        return accounts
    except Exception as e:
        print("API 지갑 불러오기 에러:", e)
        return []


async def buycoinmarket(uno, coink, setkey, amt, db: AsyncSession = Depends(get_db)):
    """빗썸 API 2.0 시장가 매수 (원화 금액 기준)"""
    try:
        key1, key2 = await getKeys(uno, setkey, db)
        # coink: "KRW-BTC" 형태
        market = coink if coink.startswith("KRW-") else f"KRW-{coink}"
        params = {
            "market": market,
            "side": "bid",  # 매수
            "price": str(amt),  # 시장가 매수 시 주문 총액(KRW)
            "ord_type": "price"  # 시장가 매수 타입
        }
        res = await bithumb_api2_request("POST", "/v1/orders", key1, key2, params=params)
        return res if ("uuid" in res or "order_id" in res) else False
    except Exception as e:
        print("시장가 매수 에러:", e)
        return False


async def sellcoinpercent(uno, coink, setkey, volm, db: AsyncSession = Depends(get_db)):
    """빗썸 API 2.0 시장가 매도 (수량 기준)"""
    try:
        key1, key2 = await getKeys(uno, setkey, db)
        accounts = await bithumb_api2_request("GET", "/v1/accounts", key1, key2)

        target_currency = coink.replace("KRW-", "").upper()
        available_balance = 0.0
        for item in accounts:
            if item.get("currency") == target_currency:
                available_balance = float(item.get("balance", 0))
                break

        if available_balance <= 0:
            return False

        sell_volume = available_balance * (float(volm) / 100.0)
        market = coink if coink.startswith("KRW-") else f"KRW-{coink}"

        params = {
            "market": market,
            "side": "ask",  # 매도
            "volume": str(sell_volume),  # 매도 수량
            "ord_type": "market"  # 시장가 매도 타입
        }
        res = await bithumb_api2_request("POST", "/v1/orders", key1, key2, params=params)
        return res if ("uuid" in res or "order_id" in res) else False
    except Exception as e:
        print("시장가 매도 에러:", e)
        return False


async def cancelorder(uno: int, setkey: str, uuid: str, db: AsyncSession = Depends(get_db)):
    """빗썸 API 2.0 주문 취소"""
    try:
        key1, key2 = await getKeys(uno, setkey, db)
        params = {"uuid": uuid}
        res = await bithumb_api2_request("DELETE", "/v1/order", key1, key2, params=params)
        return res
    except Exception as e:
        print("주문 취소 에러:", e)
        return False


async def dashcandle548(coink):
    coin = coink.replace("KRW-", "")
    candles: DataFrame | None = pybithumb.get_candlestick(coin, chart_intervals="5m")
    if candles is not None:
        return candles.tail(48)
    return None


async def tradedcoins(uno: int, db: AsyncSession = Depends(get_db)):
    try:
        sql = text(
            "select distinct bidCoin from traceSetup where userNo=:userno and attrib not like :xattr order by bidCoin asc")
        rows = await db.execute(sql, {"userno": uno, "xattr": "%XXXUP%"})
        coins = rows.fetchall()
        return [list(c) for c in coins]
    except Exception as e:
        print("거래 코인 목록 조회 에러", e)
        return []


async def get_tradelogbithumb(coink: str, userno: int, setkey: str, db: AsyncSession = Depends(get_db)):
    try:
        key1, key2 = await getKeys(userno, setkey, db)
        bithumb = pybithumb.Bithumb(key1, key2)
        coin = coink.replace("KRW-", "")
        orders = bithumb.get_order_completed(coin)
        return orders if orders else []
    except Exception as e:
        print("거래이력 불러오기 에러", e)
        return []


async def api_tradelogbithumb(coink: str, userno: int, db: AsyncSession = Depends(get_db)):
    try:
        key1, key2 = await api_getKeys(userno, db)
        bithumb = pybithumb.Bithumb(key1, key2)
        coin = coink.replace("KRW-", "")
        orders = bithumb.get_order_completed(coin)
        return orders if orders else []
    except Exception as e:
        print("거래이력 불러오기 에러", e)
        return []


async def get_orderlist(userno: int, setkey: str, slot: int, db: AsyncSession = Depends(get_db)):
    try:
        key1, key2 = await getKeys(userno, setkey, db)
        bithumb = pybithumb.Bithumb(key1, key2)
        setups = await getsetups(userno, slot, db)
        orders = []
        if setups:
            for setup in setups:
                coin = setup[6].replace("KRW-", "")
                order = bithumb.get_outstanding_order(coin)
                if order:
                    orders.extend(order)
        return orders
    except Exception as e:
        print("주문내용 불러오기 에러", e)
        return []


async def get_mtorderlist(userno: int, setkey: str, db: AsyncSession = Depends(get_db)):
    try:
        key1, key2 = await getKeys(userno, setkey, db)
        bithumb = pybithumb.Bithumb(key1, key2)
        setups = await checkwallet(userno, setkey, db)
        orders = []
        for setup in setups:
            if setup["currency"] != "KRW":
                order = bithumb.get_outstanding_order(setup["currency"])
                if order:
                    orders.extend(order)
        return orders
    except Exception as e:
        print("mt주문내용 불러오기 에러", e)
        return []


async def api_mtorderlist(userno: int, db: AsyncSession = Depends(get_db)):
    try:
        key1, key2 = await api_getKeys(userno, db)
        bithumb = pybithumb.Bithumb(key1, key2)
        setups = await api_checkwallet(userno, db)
        orders = []
        for setup in setups:
            if setup["currency"] != "KRW":
                order = bithumb.get_outstanding_order(setup["currency"])
                if order:
                    orders.extend(order)
        return orders
    except Exception as e:
        print("mt주문내용 불러오기 에러", e)
        return []


async def getsetups(uno: int, slotno: int, db: AsyncSession = Depends(get_db)):
    try:
        if slotno == 0:
            sql = text("select * from traceSetup where userNo=:userno and attrib not like :xatts")
            rows = await db.execute(sql, {"userno": uno, "xatts": '%XXXUP%'})
        else:
            sql = text("select * from traceSetup where userNo=:userno and slot = :slot and attrib not like :xattr")
            rows = await db.execute(sql, {"userno": uno, "slot": slotno, "xattr": '%XXXUP%'})
        return list(rows.fetchall())
    except Exception as e:
        print('설정 불러오기 오류', e)
        return False


async def get_mtsetups(uno: int, db: AsyncSession = Depends(get_db)):
    try:
        sql = text("select * from mtSetup where userNo=:userno and attrib not like :xatts")
        rows = await db.execute(sql, {"userno": uno, "xatts": '%XXXUP%'})
        return list(rows.fetchall())
    except Exception as e:
        print('설정 불러오기 오류', e)
        return False


async def api_mtsetups(uno: int, db: AsyncSession = Depends(get_db)):
    return await get_mtsetups(uno, db)


async def setonoffs(setno: int, yesno: str, db: AsyncSession = Depends(get_db)):
    try:
        sql = text("UPDATE mtPondSetup SET activeYN = :yesno where setupNo=:setno AND attrib not like :xattr")
        await db.execute(sql, {"setno": setno, "yesno": yesno, "xattr": '%XXXUP%'})
        await db.commit()
    except Exception as e:
        print('거래 ON/OFF 오류', e)


async def setonoff(uno: int, yesno: str, db: AsyncSession = Depends(get_db)):
    try:
        sql = text("UPDATE mtSetup SET activeYN = :yesno where userNo=:userno AND attrib not like :xattr")
        await db.execute(sql, {"userno": uno, "yesno": yesno, "xattr": '%XXXUP%'})
        await db.commit()
    except Exception as e:
        print('거래 ON/OFF 오류', e)


async def get_trsetups(uno, db: AsyncSession = Depends(get_db)):
    try:
        query = text("SELECT * FROM polarisSets where userNo = :uno and attrib not like :attxx")
        result = await db.execute(query, {"uno": uno, "attxx": "%XXX%"})
        mysetups = result.fetchall()
        return [{
            "setupNo": setup[0],
            "coinName": setup[2],
            "stepAmt": setup[3],
            "tradeType": setup[4],
            "maxAmt": setup[5],
            "useYN": setup[6],
        } for setup in mysetups]
    except Exception as e:
        print("Get Setup Error!!", e)
        raise HTTPException(status_code=500, detail="Internal Server Error")


async def setautostop(sno: int, yesno: str, db: AsyncSession = Depends(get_db)):
    try:
        sql = text("UPDATE traceSetup SET doubleYN = :yesno where setupNo=:sno")
        await db.execute(sql, {"sno": sno, "yesno": yesno})
        await db.commit()
    except Exception as e:
        print('자동 멈춤 기능 설정 오류', e)


async def setlconoff(setno: int, lcrate: float, yesno: str, db: AsyncSession = Depends(get_db)):
    try:
        sql = text("UPDATE mtPondSetup SET losscut = :lcrate, lcYN = :yesno where setupNo=:setno")
        await db.execute(sql, {"lcrate": lcrate, "yesno": yesno, "setno": setno})
        await db.commit()
    except Exception as e:
        print('손절 ONOFF 오류', e)


async def selectsetlist(db: AsyncSession = Depends(get_db)):
    try:
        sql = text("SELECT * FROM traceSets WHERE useYN = :useyn and attrib NOT LIKE :xattr")
        rows = await db.execute(sql, {"useyn": "Y", "xattr": "%XXXUP%"})
        return rows.fetchall()
    except Exception as e:
        print('트레이딩 설정 목록 불러오기 오류', e)
        return []


async def erasebid(uno: int, setkey: str, tabindex: int, db: AsyncSession = Depends(get_db)):
    try:
        sql2 = text("update traceSetup set attrib=:xattr where userNo=:userno and slot = :slot")
        await db.execute(sql2, {"xattr": "XXXUPXXXUPXXXUP", "userno": uno, "slot": tabindex})
        await db.commit()
        return True
    except Exception:
        return False


async def erasemtpondsetup(uno: int, setkey: str, db: AsyncSession = Depends(get_db)):
    try:
        sql1 = text("update mtSetup set attrib=:xattr where userNo=:userno")
        await db.execute(sql1, {"xattr": "XXXUPXXXUP", "userno": uno})
        await db.commit()
        return True
    except Exception:
        return False


async def update_userdtl(uno: int, key1: str, key2: str, svrno: int, db: AsyncSession = Depends(get_db)):
    try:
        sql2 = text("update traceUser set apiKey1 = :key1 , apiKey2 = :key2, serverNo = :svrno where userNo=:userno")
        await db.execute(sql2, {"key1": key1, "key2": key2, "svrno": svrno, "userno": uno})
        await db.commit()
        return True
    except Exception:
        return False


async def setupbid(uno: int, setkey: str, initbid: float, bidstep: int, bidrate: float, askrate: float, coinn: str,
                   svrno: int, tradeset: int, holdNo: int, doubleYN: str, limitamt: float, limityn: str, slot: int,
                   db: AsyncSession = Depends(get_db)):
    chkkey = await check_setkey(uno, setkey, db)
    if chkkey:
        try:
            sql = text("""
                       insert into traceSetup 
                       (userNo, initAsset, bidInterval, bidRate, askrate, bidCoin, custKey, serverNo, holdNo, doubleYN,
                        limitAmt, limitYN, slot, regDate)
                       VALUES (:uno, :initbid, :bidstep, :bidrate, :askrate, :coinn, :tradeset, :svrno, :holdNo,
                               :doubleYN, :limitamt, :limityn, :slot, now())
                       """)
            await db.execute(sql, {
                "uno": uno, "initbid": initbid, "bidstep": bidstep,
                "bidrate": bidrate, "askrate": askrate, "coinn": coinn,
                "tradeset": tradeset, "svrno": svrno, "holdNo": holdNo,
                "doubleYN": doubleYN, "limitamt": limitamt, "limityn": limityn, "slot": slot,
            })
            await db.commit()
            return True
        except Exception as e:
            print('트레이딩 설정 저장 오류', e)
    return False


async def setupmymtpondset(uno: int, setkey: str, initbid: float, addbid: float, limitbid: float, minmargin: float,
                           cutrate: float, db: AsyncSession = Depends(get_db)):
    chkkey = await check_setkey(uno, setkey, db)
    if chkkey:
        try:
            sql = text("""
                       insert into mtSetup (userNo, initAmt, addAmt, limitAmt, minMargin, lcRate)
                       VALUES (:userNo, :iniBid, :addBid, :limitBid, :minMargin, :losscut)
                       """)
            await db.execute(sql, {
                "userNo": uno, "iniBid": initbid, "addBid": addbid, "limitBid": limitbid, "minMargin": minmargin,
                "losscut": cutrate})
            await db.commit()
            return True
        except Exception as e:
            print('mtPond 트레이딩 설정 저장 오류', e)
    return False


async def editbidsetup(sno: int, uno: int, setkey: str, initbid: float, bidstep: int, bidrate: float, askrate: float,
                       coinn: str, svrno: int, tradeset: int, holdNo: int, doubleYN: str, limitamt: float, limityn: str,
                       slot: int, db: AsyncSession = Depends(get_db)):
    chkkey = await check_setkey(uno, setkey, db)
    if chkkey:
        try:
            sqlp = text("update traceSetup set attrib=:xattr where setupNo=:sno")
            await db.execute(sqlp, {"sno": sno, "xattr": "XXXUPXXXUPXXXUP"})
            await db.commit()
            sql = text("""
                       insert into traceSetup
                       (userNo, initAsset, bidInterval, bidRate, askrate, bidCoin, custKey, serverNo, holdNo, doubleYN,
                        limitAmt, limitYN, slot, regDate)
                       VALUES (:uno, :initbid, :bidstep, :bidrate, :askrate, :coinn, :tradeset, :svrno, :holdNo,
                               :doubleYN, :limitamt, :limityn, :slot, now())
                       """)
            await db.execute(sql, {
                "uno": uno, "initbid": initbid, "bidstep": bidstep,
                "bidrate": bidrate, "askrate": askrate, "coinn": coinn,
                "tradeset": tradeset, "svrno": svrno, "holdNo": holdNo,
                "doubleYN": doubleYN, "limitamt": limitamt, "limityn": limityn, "slot": slot,
            })
            await db.commit()
            return True
        except Exception as e:
            print('접속오류', e)
    return False


# ==========================================
# 라우트 핸들러
# ==========================================

@app.get("/")
async def root(request: Request):
    return templates.TemplateResponse(
        request=request,
        name="login/login.html",
        context={}
    )


@app.post("/loginchk")
async def login(request: Request, response: Response, uid: str = Form(...), upw: str = Form(...),
                db: AsyncSession = Depends(get_db)):
    user = await selectUsers(uid, upw, db)
    if user is None or user[0] is None:
        return templates.TemplateResponse(
            request=request,
            name="login/login.html",
            context={"error": "Invalid credentials"}
        )

    await setKeys(user[0][0], user[1], db)
    request.session["user_No"] = user[0][0]
    request.session["user_Name"] = user[0][1]
    request.session["server_No"] = user[0][2]
    request.session["user_Role"] = user[0][3]
    request.session["License"] = user[0][4]
    request.session["setKey"] = user[1]
    return RedirectResponse(url=f"/balance/{user[0][0]}", status_code=303)


@app.get("/logout")
async def logout(request: Request):
    request.session.clear()
    return RedirectResponse(url="/")


@app.get("/hotcoin_list/{uno}")
async def hotcoinlist(request: Request, uno: int, user_session: int = Depends(require_login),
                      db: AsyncSession = Depends(get_db)):
    if uno != user_session:
        return RedirectResponse(url="/", status_code=303)
    usern = request.session.get("user_Name")
    setkey = request.session.get("setupKey")
    try:
        orderbooks = await get_hotcoins(request, db)
        hotamt = await get_hotamt(request, db)
        time_diff = ""
        is_reloadable = "N"
        if orderbooks and len(orderbooks) > 0:
            gettime = orderbooks[0][8]
            diff = datetime.now() - gettime
            days = diff.days
            hours = diff.seconds // 3600
            minutes = (diff.seconds % 3600) // 60
            seconds = diff.seconds % 60
            time_diff = f"{days}일 {hours}시간 {minutes}분 {seconds}초 "
            is_reloadable = "Y" if diff.total_seconds() > 10800 else "N"
        trsetups = await get_trsetups(uno, db)
        return templates.TemplateResponse(
            request=request,
            name="trade/hotcoinlist.html",
            context={
                "userNo": uno, "user_No": uno, "userName": usern,
                "setkey": setkey, "orderbooks": orderbooks, "time_diff": time_diff,
                "trsetups": trsetups, "reloadable": is_reloadable, "hotamt": hotamt,
            }
        )
    except Exception as e:
        print("Get Hotcoins Error !!", e)


@app.get("/balance/{uno}")
async def my_balance(request: Request, uno: int, user_session: int = Depends(require_login),
                     db: AsyncSession = Depends(get_db)):
    if uno != user_session:
        return RedirectResponse(url="/", status_code=303)
    try:
        setKey = request.session.get("setKey")
        userName = request.session.get("user_Name")
        userRole = request.session.get("user_Role")
        userLicense = request.session.get("License")
        mycoins = await checkwallet(uno, setKey, db)
        cprices = await get_current_prices()
    except Exception as e:
        print("Get Balances Error !!", e)
        mycoins = []
        cprices = []
    return templates.TemplateResponse(
        request=request,
        name="wallet/mywallet.html",
        context={
            "user_No": uno, "user_Name": userName, "user_Role": userRole,
            "setkey": setKey, "license": userLicense, "mycoins": mycoins, "myavgp": None, "cuprices": cprices
        }
    )


@app.post("/tradebuymarket/{uno}/{setkey}/{coinn}/{costk}")
async def tradebuymarket(request: Request, uno: int, setkey: str, coinn: str, costk: float,
                         user_session: int = Depends(require_login), db: AsyncSession = Depends(get_db)):
    if uno != user_session or int(setkey) != int(request.session.get("setKey", -1)):
        return JSONResponse({"success": False, "message": "권한이 없습니다.", "redirect": "/"})
    try:
        butm = await buycoinmarket(uno, coinn, setkey, costk, db)
        return JSONResponse({"success": True if butm else False, "redirect": f"/balance/{uno}"})
    except Exception as e:
        print("Error!!", e)
        return JSONResponse({"success": False, "message": "서버 오류", "redirect": f"/balance/{uno}"})


@app.post("/tradesellmarket/{uno}/{setkey}/{coinn}/{volm}")
async def tradesellmarket(request: Request, uno: int, setkey: str, coinn: str, volm: float,
                          user_session: int = Depends(require_login), db: AsyncSession = Depends(get_db)):
    if uno != user_session or int(setkey) != int(request.session.get("setKey", -1)):
        return JSONResponse({"success": False, "message": "권한이 없습니다.", "redirect": "/"})
    try:
        sellm = await sellcoinpercent(uno, coinn, setkey, volm, db)
        return JSONResponse({"success": True if sellm else False, "redirect": f"/balance/{uno}"})
    except Exception as e:
        print("Error!!", e)
        return JSONResponse({"success": False, "message": "서버 오류", "redirect": f"/balance/{uno}"})


@app.get('/tradedetail/{userno}/{setkey}')
async def tradedetail(request: Request, userno: int, setkey: str, db: AsyncSession = Depends(get_db)):
    coinlist = pybithumb.get_tickers(fiat="KRW")
    trcoins = await tradedcoins(userno, db)
    userName = request.session.get("user_Name")
    userRole = request.session.get("user_Role")
    return templates.TemplateResponse(
        request=request,
        name="trade/mytradingresult.html",
        context={
            "coinlist": coinlist, "trcoins": trcoins, "user_No": userno,
            "user_Name": userName, "user_Role": userRole, "setkey": setkey, "reqitems": [], "dates": []
        }
    )


@app.get('/tradedetails/{userno}/{setkey}/{coink}')
async def tradedetails(request: Request, userno: int, setkey: str, coink: str, db: AsyncSession = Depends(get_db)):
    coinlist = pybithumb.get_tickers(fiat="KRW")
    trcoins = await tradedcoins(userno, db)
    trlogs = await get_tradelogbithumb(coink, userno, setkey, db)
    dates = []
    if isinstance(trlogs, list) and trlogs:
        dates = list({item.get('transaction_date', '')[:10] for item in trlogs if
                      isinstance(item, dict) and 'transaction_date' in item})
        dates.sort(reverse=True)
    userName = request.session.get("user_Name")
    userRole = request.session.get("user_Role")
    return templates.TemplateResponse(
        request=request,
        name="trade/mytradingresult.html",
        context={
            "coinlist": coinlist, "trcoins": trcoins, "user_No": userno,
            "user_Name": userName, "user_Role": userRole, "setkey": setkey, "reqitems": trlogs, "dates": dates,
            "coink": coink
        }
    )


@app.get('/tradetrend/{userno}/{setkey}')
async def tradetrend(request: Request, userno: int, setkey: str, db: AsyncSession = Depends(get_db)):
    trcoins = await tradedcoins(userno, db)
    mycoins = await checkwallet(userno, setkey, db)
    userName = request.session.get("user_Name")
    userRole = request.session.get("user_Role")
    return templates.TemplateResponse(
        request=request,
        name="trade/mytradingtrend.html",
        context={
            "trcoins": trcoins, "user_No": userno, "user_Name": userName,
            "user_Role": userRole, "setkey": setkey, "mycoins": mycoins
        }
    )


@app.get('/bithumbtradetrend/{userno}/{setkey}')
async def bithumbtradetrend(request: Request, userno: int, setkey: str, db: AsyncSession = Depends(get_db)):
    hotcoins = await get_hotcoins(request, db)
    trcoins = [row[3] for row in hotcoins] if hotcoins else []
    userName = request.session.get("user_Name")
    userRole = request.session.get("user_Role")
    return templates.TemplateResponse(
        request=request,
        name="trade/bithumbtradingtrend.html",
        context={
            "trcoins": trcoins, "user_No": userno, "user_Name": userName, "user_Role": userRole,
            "setkey": setkey
        }
    )


@app.get('/settletrend/{userno}/{setkey}')
async def settletrend(request: Request, userno: int, setkey: str, db: AsyncSession = Depends(get_db)):
    trcoins = await tradedcoins(userno, db)
    mycoins = await checkwallet(userno, setkey, db)
    userName = request.session.get("user_Name")
    userRole = request.session.get("user_Role")
    return templates.TemplateResponse(
        request=request,
        name="trade/mysettletrend.html",
        context={
            "trcoins": trcoins, "user_No": userno, "user_Name": userName, "user_Role": userRole,
            "setkey": setkey, "mycoins": mycoins
        }
    )


@app.get('/bithumbsettletrend/{userno}/{setkey}')
async def bithumbsettletrend(request: Request, userno: int, setkey: str, db: AsyncSession = Depends(get_db)):
    hotcoins = await get_hotcoins(request, db)
    trcoins = [row[3] for row in hotcoins] if hotcoins else []
    userName = request.session.get("user_Name")
    userRole = request.session.get("user_Role")
    return templates.TemplateResponse(
        request=request,
        name="trade/bithumbsettletrend.html",
        context={
            "trcoins": trcoins, "user_No": userno, "user_Name": userName, "user_Role": userRole,
            "setkey": setkey
        }
    )


@app.get('/userEdit/{userno}/{setkey}')
async def useredit(request: Request, userno: int, setkey: str, db: AsyncSession = Depends(get_db)):
    userName = request.session.get("user_Name")
    userRole = request.session.get("user_Role")
    userdtl = await get_userdetail(userno, setkey, db)
    return templates.TemplateResponse(
        request=request,
        name="login/userDtl.html",
        context={
            "user_No": userno, "user_Name": userName, "user_Role": userRole, "setkey": setkey,
            "userdtl": userdtl
        }
    )


@app.get('/rest_getwallet/{userno}/{setkey}')
async def restgetwallet(request: Request, userno: int, setkey: str, db: AsyncSession = Depends(get_db)):
    try:
        mycoins = await checkwallet(userno, setkey, db)
        return JSONResponse({"success": True, "data": mycoins})
    except Exception as e:
        print("Error!!", e)
        return JSONResponse({"success": False, "data": []})


@app.get('/mytradestat/{userno}/{setkey}/{slot}')
async def mytradestat(request: Request, userno: int, setkey: str, slot: int, user_session: int = Depends(require_login),
                      db: AsyncSession = Depends(get_db)):
    try:
        setups = await getsetups(userno, slot, db)
        userName = request.session.get("user_Name")
        userRole = request.session.get("user_Role")
        userLicense = request.session.get("License")
        mycoins = await checkwallet(userno, setkey, db)
        orderlist = await get_orderlist(userno, setkey, slot, db)
        return templates.TemplateResponse(
            request=request,
            name="trade/mytrademain.html",
            context={
                "setups": setups, "user_No": userno, "user_Name": userName,
                "user_Role": userRole, "setkey": setkey, "license": userLicense, "mycoins": mycoins,
                "slot": slot, "orderlist": orderlist
            }
        )
    except Exception as e:
        print("트레이딩 상태 불러오기 에러", e)


@app.get('/mymtpondstat/{userno}/{setkey}')
async def mymtpondstat(request: Request, userno: int, setkey: str, user_session: int = Depends(require_login),
                       db: AsyncSession = Depends(get_db)):
    try:
        userName = request.session.get("user_Name")
        userRole = request.session.get("user_Role")
        userLicense = request.session.get("License")
        onoffstat = await get_onoff(userno, db)
        mysettings = await get_mtsetups(userno, db)
        mycoins = await checkwallet(userno, setkey, db)
        myorders = await get_mtorderlist(userno, setkey, db)
        return templates.TemplateResponse(
            request=request,
            name="trade/mypondmain.html",
            context={
                "user_No": userno, "user_Name": userName, "user_Role": userRole,
                "setkey": setkey, "license": userLicense, "onoffstat": onoffstat[0] if onoffstat else "N",
                "mysettings": mysettings, "myorders": myorders, "mycoins": mycoins
            }
        )
    except Exception as e:
        print("mtPond 트레이딩 상태 불러오기 에러", e)


@app.get('/mytradeSet/{userno}')
async def mytradeSet(request: Request, userno: int, db: AsyncSession = Depends(get_db)):
    coinlist = pybithumb.get_tickers(fiat="KRW")
    userName = request.session.get("user_Name")
    userRole = request.session.get("user_Role")
    setkey = request.session.get("setKey")
    trcnt = request.session.get("License")
    serverno = request.session.get("server_No")
    setlist = await selectsetlist(db)
    return templates.TemplateResponse(
        request=request,
        name="trade/setmytrades.html",
        context={
            "coinlist": coinlist, "setlist": setlist, "trcnt": trcnt,
            "user_Name": userName, "setkey": setkey, "user_No": userno, "user_Role": userRole, "server_No": serverno
        }
    )


@app.get('/mymtpondSet/{userno}')
async def mypondSet(request: Request, userno: int, db: AsyncSession = Depends(get_db)):
    coinlist = pybithumb.get_tickers(fiat="KRW")
    userName = request.session.get("user_Name")
    userRole = request.session.get("user_Role")
    setkey = request.session.get("setKey")
    trcnt = request.session.get("License")
    serverno = request.session.get("server_No")
    return templates.TemplateResponse(
        request=request,
        name="trade/setmtpond.html",
        context={
            "coinlist": coinlist, "trcnt": trcnt, "user_Name": userName,
            "setkey": setkey, "user_No": userno, "user_Role": userRole, "server_No": serverno
        }
    )


@app.get('/editSetup')
async def editSetup(
        request: Request,
        setno: str = Query(...), coinA: str = Query(...), coinB: str = Query(...),
        tabindex: str = Query(...), db: AsyncSession = Depends(get_db)
):
    coinlist = pybithumb.get_tickers(fiat="KRW")
    setlist = await selectsetlist(db)
    return templates.TemplateResponse(
        request=request,
        name="trade/editmytrade.html",
        context={
            "coinlist": coinlist, "setno": setno, "coinA": coinA,
            "coinB": coinB, "setlist": setlist, "tabindex": tabindex,
            "setkey": request.session.get("setKey"), "user_No": request.session.get("user_No"),
            "user_Name": request.session.get("user_Name"), "user_Role": request.session.get("user_Role"),
            "server_No": request.session.get("server_No"),
        }
    )


@app.post("/setupbids")
async def setupmybids(
        userno: str = Form(...), tabindex: str = Form(...), initprice: str = Form(...),
        lcrate: Optional[str] = Form(None), lcchk: Optional[str] = Form(None),
        tradeset: str = Form(...), coinn1: Optional[str] = Form(None),
        coinn2: Optional[str] = Form(None), coinn3: Optional[str] = Form(None),
        setkey: str = Form(...), svrno: str = Form(...),
        limityn: Optional[str] = Form(None), limitamt: Optional[str] = Form(None),
        db: AsyncSession = Depends(get_db),
):
    uno = int(userno)
    slot = int(tabindex)
    price = initprice.replace(',', '') if initprice else ''
    tradeset_split = tradeset.split(',')
    tradeset_val = tradeset_split[0]
    bidsetps = tradeset_split[1] if len(tradeset_split) > 1 else None
    hno = tradeset_split[1] if len(tradeset_split) > 1 else None
    dyn = 'Y' if limityn == 'on' else 'N'
    limityn_value = 'Y' if limityn == 'on' else 'N'
    lmtamt = (limitamt or '').replace(',', '') if limitamt else ''
    bidrate = 1.0 if lcchk == 'on' else 0.0
    await erasebid(uno, setkey, slot, db)
    for coin in [coinn1, coinn2, coinn3]:
        if coin:
            await setupbid(
                uno, setkey, price, bidsetps, bidrate, lcrate, coin, svrno,
                tradeset_val, hno, dyn, lmtamt, limityn_value, slot, db
            )
    return RedirectResponse(url=f"/mytradestat/{uno}/{setkey}/{slot}", status_code=303)


@app.post("/setupmtponds")
async def setupmtponds(
        userno: str = Form(...), initprice: str = Form(...), addprice: str = Form(...),
        limitamt: str = Form(...), minmargin: str = Form(...),
        lcrate: Optional[str] = Form(None), setkey: str = Form(...),
        db: AsyncSession = Depends(get_db),
):
    uno = int(userno)
    initprice = initprice.replace(',', '') if initprice else '0'
    addprice = addprice.replace(',', '') if addprice else '0'
    limitamt = limitamt.replace(',', '') if limitamt else '0'
    lcrate = lcrate or '0'
    minmargin = minmargin.replace(',', '') if minmargin else ''
    await erasemtpondsetup(uno, setkey, db)
    await setupmymtpondset(uno, setkey, initprice, addprice, limitamt, minmargin, lcrate, db)
    return RedirectResponse(url=f"/mymtpondstat/{uno}/{setkey}", status_code=303)


@app.post("/setupbid")
async def setupmybid(
        setno: str = Form(...), userno: str = Form(...), slot: str = Form(...),
        coinn: str = Form(...), initprice: str = Form(...),
        lcrate: Optional[str] = Form(None), lcchk: Optional[str] = Form(None),
        tradeset: str = Form(...), setkey: str = Form(...), svrno: str = Form(...),
        limityn: Optional[str] = Form(None), limitamt: Optional[str] = Form(None),
        db: AsyncSession = Depends(get_db),
):
    tradeset_split = tradeset.split(',')
    await editbidsetup(
        int(setno), int(userno), setkey,
        initprice.replace(',', '') if initprice else '',
        tradeset_split[1] if len(tradeset_split) > 1 else None,
        1.0 if lcchk == 'on' else 0.0,
        lcrate, coinn, int(svrno), tradeset_split[0],
        tradeset_split[1] if len(tradeset_split) > 1 else None,
        'Y' if limityn == 'on' else 'N',
        (limitamt or '').replace(',', '') if limitamt else '',
        'Y' if limityn == 'on' else 'N',
        int(slot), db
    )
    return RedirectResponse(url=f"/mytradestat/{userno}/{setkey}/{slot}", status_code=303)


@app.post("/changemypass")
async def change_password(data: dict = Body(...), db: AsyncSession = Depends(get_db)):
    sql = text("UPDATE traceUser SET userPasswd = PASSWORD(:passwd) WHERE userNo = :userno")
    await db.execute(sql, {"passwd": data["passwd"], "userno": data["uno"]})
    await db.commit()
    return {"result": "success"}


@app.post("/updateuserdtl")
async def update_userdetail(
        request: Request, uno: str = Form(...), apikey1: str = Form(...),
        apikey2: str = Form(...), svrno: str = Form(...), db: AsyncSession = Depends(get_db),
):
    setkey = request.session.get("setKey")
    await update_userdtl(int(uno), apikey1, apikey2, int(svrno), db)
    return RedirectResponse(url=f"/userEdit/{uno}/{setkey}", status_code=303)


@app.get('/rest_getorder/{userno}/{setkey}/{slot}')
async def restgetorder(request: Request, userno: int, setkey: str, slot: int, db: AsyncSession = Depends(get_db)):
    try:
        orderlist = await get_orderlist(userno, setkey, slot, db)
        return JSONResponse({"success": True, "data": orderlist})
    except Exception as e:
        print("Error!!", e)
        return JSONResponse({"success": False, "data": []})


@app.get('/rest_getmtorder/{userno}/{setkey}')
async def restgetmtorder(request: Request, userno: int, setkey: str, db: AsyncSession = Depends(get_db)):
    try:
        orderlist = await get_mtorderlist(userno, setkey, db)
        return JSONResponse({"success": True, "data": orderlist})
    except Exception as e:
        print("Error!!", e)
        return JSONResponse({"success": False, "data": []})


@app.post('/cancelOrder')
async def cancelorder_route(request: Request, uno: int = Form(...), setkey: str = Form(...), uuid: str = Form(...),
                            db: AsyncSession = Depends(get_db)):
    try:
        key1, key2 = await getKeys(uno, setkey, db)
        bithumb = pybithumb.Bithumb(key1, key2)
        order = bithumb.cancel_order(uuid)
        return JSONResponse({"success": True, "data": order})
    except Exception as e:
        print("주문취소 에러", e)
        return JSONResponse({"success": False, "message": str(e)})


@app.post('/setyns')
async def setyns(request: Request, setno: int = Form(...), yn: str = Form(...), db: AsyncSession = Depends(get_db)):
    try:
        await setonoffs(setno, yn, db)
        return JSONResponse({"success": True, "data": yn})
    except Exception:
        return JSONResponse({"success": False, "data": yn})


@app.post('/setautostop')
async def setatstop(request: Request, sno: int = Form(...), yesno: str = Form(...), db: AsyncSession = Depends(get_db)):
    try:
        await setautostop(sno, yesno, db)
        return JSONResponse({"success": True, "data": yesno})
    except Exception:
        return JSONResponse({"success": False, "data": yesno})


@app.post('/setlosscut')
async def setlosscut(request: Request, sno: int = Form(...), rate: float = Form(...), onoff: float = Form(...),
                     db: AsyncSession = Depends(get_db)):
    try:
        await setlconoff(sno, rate, onoff, db)
        return JSONResponse({"success": True, "data": rate})
    except Exception:
        return JSONResponse({"success": False, "data": rate})


# ==========================================
# 빗썸 공식 웹소켓 스트림 연동
# ==========================================

async def bithumb_ws_price_stream(symbols: list):
    """빗썸 실시간 Ticker WebSocket 구독 (symbols: ['BTC', 'ETH'] 등)"""
    uri = "wss://pubwss.bithumb.com/pub/ws"
    formatted_symbols = [f"{s.replace('KRW-', '')}_KRW" for s in symbols]
    subscribe_data = {
        "type": "ticker",
        "symbols": formatted_symbols,
        "tickTypes": ["MID"]
    }
    async with websockets.connect(uri, ping_interval=60) as websocket:
        await websocket.send(json.dumps(subscribe_data))
        while True:
            data = await websocket.recv()
            parsed = json.loads(data)
            if parsed.get("type") == "ticker" and "content" in parsed:
                content = parsed["content"]
                symbol = content.get("symbol", "").replace("_KRW", "")
                market = f"KRW-{symbol}"
                current_price = float(content.get("closePrice", 0))
                change = content.get("chgRate", 0)
                yield market, current_price, change


@app.websocket("/ws/coinprice")
async def coin_price_ws(websocket: WebSocket):
    await websocket.accept()
    coins = websocket.query_params.get("coins", "")
    coin_list = coins.split(",") if coins else []
    try:
        async for market, current_price, change in bithumb_ws_price_stream(coin_list):
            await websocket.send_json({"market": market, "current_price": current_price, "change": change})
    except WebSocketDisconnect:
        pass
    except Exception as e:
        print("WebSocket Error:", e)


async def bithumb_ws_orderbook_stream(symbols: list):
    """빗썸 실시간 호가 WebSocket 구독"""
    uri = "wss://pubwss.bithumb.com/pub/ws"
    formatted_symbols = [f"{s.replace('KRW-', '')}_KRW" for s in symbols]
    subscribe_data = {
        "type": "orderbookdepth",
        "symbols": formatted_symbols
    }
    async with websockets.connect(uri, ping_interval=60) as websocket:
        await websocket.send(json.dumps(subscribe_data))
        while True:
            data = await websocket.recv()
            parsed = json.loads(data)
            if parsed.get("type") == "orderbookdepth" and "content" in parsed:
                content = parsed["content"]
                symbol = content.get("symbol", "").replace("_KRW", "")
                units = []
                for ask, bid in zip(content.get("asks", []), content.get("bids", [])):
                    units.append({
                        "ask_price": float(ask[0]),
                        "ask_size": float(ask[1]),
                        "bid_price": float(bid[0]),
                        "bid_size": float(bid[1])
                    })
                yield {
                    "market": f"KRW-{symbol}",
                    "orderbook_units": units
                }


@app.websocket("/ws/orderbook")
async def coin_orderbook_ws(websocket: WebSocket):
    await websocket.accept()
    coins = websocket.query_params.get("coins", "")
    coin_list = coins.split(",") if coins else []
    try:
        async for ob_data in bithumb_ws_orderbook_stream(coin_list):
            await websocket.send_json(ob_data)
    except WebSocketDisconnect:
        pass
    except Exception as e:
        print("WebSocket Error:", e)


async def bithumb_ws_trade_stream(symbols: list):
    """빗썸 실시간 체결 WebSocket 구독"""
    uri = "wss://pubwss.bithumb.com/pub/ws"
    formatted_symbols = [f"{s.replace('KRW-', '')}_KRW" for s in symbols]
    subscribe_data = {
        "type": "transaction",
        "symbols": formatted_symbols
    }
    async with websockets.connect(uri, ping_interval=60) as websocket:
        await websocket.send(json.dumps(subscribe_data))
        while True:
            data = await websocket.recv()
            parsed = json.loads(data)
            if parsed.get("type") == "transaction" and "content" in parsed:
                for item in parsed["content"].get("list", []):
                    symbol = item.get("symbol", "").replace("_KRW", "")
                    yield {
                        "market": f"KRW-{symbol}",
                        "trade_price": float(item.get("price", 0)),
                        "trade_volume": float(item.get("units", 0)),
                        "ask_bid": "BID" if item.get("buySellGb") == "1" else "ASK",
                        "trade_time": item.get("contTime"),
                        "trade_timestamp": item.get("contDtm")
                    }


@app.websocket("/ws/trade")
async def coin_trade_ws(websocket: WebSocket):
    await websocket.accept()
    coins = websocket.query_params.get("coins", "")
    coin_list = coins.split(",") if coins else []
    try:
        async for trade_data in bithumb_ws_trade_stream(coin_list):
            await websocket.send_json(trade_data)
    except WebSocketDisconnect:
        pass
    except Exception as e:
        print("WebSocket Error:", e)


# ==========================================
# 기타 API 및 마감 라우트
# ==========================================

@app.get('/bithumbtop30/{uno}/{setkey}')
async def bithumbtop30(request: Request, uno: int, setkey: str, db: AsyncSession = Depends(get_db)):
    coins = pybithumb.get_tickers(fiat="KRW")
    return templates.TemplateResponse(
        request=request,
        name="trade/bithumbtop30.html",
        context={
            "trcnt": request.session.get("License"),
            "user_Name": request.session.get("user_Name"), "setkey": setkey, "user_No": uno,
            "user_Role": request.session.get("user_Role"), "coins": coins,
            "server_No": request.session.get("server_No")
        }
    )


@app.get('/api/mtpondsetup/{userno}')
async def mtpondsetup_all(userno: int, db: AsyncSession = Depends(get_db)):
    sql = text(
        "SELECT activeYN,initAmt,addAmt,limitAmt,minMargin,maxMargin,tickRate,tickYN,lcRate,lcGap,maxCoincnt, martinYN, stopYN, stopAutoYN FROM mtSetup WHERE userNo = :userno AND attrib NOT LIKE :attrib")
    result = await db.execute(sql, {"userno": userno, "attrib": "%XXX%"})
    rows = result.fetchall()
    data = [dict(r._mapping) for r in rows]
    return jsonable_encoder(data)


@app.post("/api/mtpondsetonoff/{userno}/{active}")
async def toggle_active_simple(userno: int, active: str, db: AsyncSession = Depends(get_db)):
    active_norm = active.strip().upper()
    if active_norm not in ("Y", "N"):
        raise HTTPException(status_code=400, detail="active 값은 Y 또는 N 이어야 합니다.")
    await setonoff(userno, active_norm, db)
    return {"userNo": userno, "activeYN": active_norm, "updated": True}


@app.post('/api/myorders/{userno}/{setkey}')
async def myorders(userno: int, setkey: str, db: AsyncSession = Depends(get_db)):
    try:
        orders = await api_mtorderlist(userno, db)
        cprices = await get_current_prices()
        return JSONResponse({"success": True, "data": orders, "cprices": cprices})
    except Exception:
        return JSONResponse({"success": False, "data": [], "cprices": []})


@app.get("/phapp/mlogin/{userid}/{passwd}")
async def mlogin(userid: str, passwd: str, db: AsyncSession = Depends(get_db)):
    try:
        query = text(
            "SELECT userNo, userName,setupKey from traceUser where userId = :userid and userPasswd = PASSWORD(:passwd)")
        r = await db.execute(query, {"userid": userid, "passwd": passwd})
        rows = r.fetchone()
        if rows is None:
            return {"error": "No data found for the given data."}
        return {"userno": rows[0], "username": rows[1], "setupkey": rows[2]}
    except Exception as e:
        print("mLogin error", e)
        return None


@app.get("/api/balance/{userno}/{setkey}")
async def api_my_balance(request: Request, userno: int, setkey: str, db: AsyncSession = Depends(get_db)):
    try:
        mycoins = await checkwallet(userno, setkey, db)
        cprices = await get_current_prices()
        return {
            "success": True,
            "userNo": userno,
            "mycoins": mycoins,
            "cuprices": cprices
        }
    except Exception as e:
        print("Get API Balances Error !!", e)
        raise HTTPException(status_code=500, detail="지갑 정보를 불러오는데 실패했습니다.")


@app.get("/excoinlist/{userNo}/{setkey}")
async def excoin(request: Request, userNo: int, setkey: str, db: AsyncSession = Depends(get_db)):
    coinlist = pybithumb.get_tickers(fiat="KRW")
    excoinlist = []
    try:
        query = text("SELECT DISTINCT market FROM exCoinlist WHERE userNo in (0, :userno) and attrib NOT LIKE :attrib")
        r = await db.execute(query, {"userno": userNo, "attrib": "%XXX%"})
        rows = r.fetchall()
        if rows:
            excoinlist = [r[0] for r in rows]
    except Exception as e:
        print("excoinlist error", e)
    return templates.TemplateResponse(
        request=request,
        name="trade/excoin.html",
        context={
            "user_No": userNo, "setkey": setkey,
            "coinlist": coinlist, "excoinlist": excoinlist
        }
    )


@app.post("/setexCoin/{userNo}")
async def setexcoin(request: Request, userNo: int, selcoin: Optional[List[str]] = Form(default=None, alias="selcoin[]"),
                    db: AsyncSession = Depends(get_db)):
    selcoin = selcoin or []
    try:
        query = text("UPDATE exCoinlist set attrib = :attx WHERE userNo = :userno")
        await db.execute(query, {"attx": "XXXUPXXXUP", "userno": userNo})
        for coin in selcoin:
            query = text("INSERT INTO exCoinlist (userNo, market) values (:userNo, :market)")
            await db.execute(query, {"userNo": userNo, "market": coin})
        await db.commit()
        return RedirectResponse(url=f"/excoinlist/{userNo}/{request.session.get('setKey')}", status_code=303)
    except Exception as e:
        print("setexcoin error", e)