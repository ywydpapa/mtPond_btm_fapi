from fastapi import FastAPI
import random
import dotenv
import os
import asyncio
import time
from datetime import datetime
from typing import Dict
import math
import aiohttp
from fastapi import FastAPI, Depends, Request, Form, Response, HTTPException, status, File, UploadFile, Query, Body
from fastapi.encoders import jsonable_encoder
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from starlette.middleware.sessions import SessionMiddleware
from fastapi.staticfiles import StaticFiles
import jinja2
import dotenv
import pyupbit
import requests
from fastapi import WebSocket, WebSocketDisconnect
import httpx
import websockets
import json
from sqlalchemy import text
from pandas import DataFrame
from typing import Optional, List


dotenv.load_dotenv()
DATABASE_URL = os.getenv("dburl")
engine = create_async_engine(DATABASE_URL, echo=True)
async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

app = FastAPI()

app.add_middleware(SessionMiddleware, secret_key="supersecretkey")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 모든 도메인 허용
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

async def get_current_prices():
    server_url = "https://api.upbit.com"
    params = {"quote_currencies": "KRW"}
    res = requests.get(server_url + "/v1/ticker/all", params=params)
    data = res.json()
    result = []
    for item in data:
        market = item.get("market")
        trade_price = item.get("trade_price")
        if market and trade_price:
            result.append({"market": market, "trade_price": trade_price})
    return result

async def get_current_price(coink):
    url = "https://api.upbit.com/v1/ticker"
    params = {"markets": coink}
    async with httpx.AsyncClient() as client:
        res = await client.get(url, params=params)
        data = res.json()
        if data and isinstance(data, list):
            item = data[0]
            return item.get("trade_price")
        return None

async def selectUsers(uid:str, upw:str,  db: AsyncSession = Depends(get_db)):
    row = None
    setkey = None
    try:
        sql = text("SELECT userNo, userName, serverNo, userRole, tradeCnt FROM traceUser WHERE userPasswd=password(:passwd) AND userId=:userid AND attrib NOT LIKE :xattr")
        result = await db.execute(sql,{"passwd":upw,"userid":uid,"xattr":"%XXXUP%"})
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
        result = db.execute(sql, {"xattr": "%XXXUP%"})
        rows = result.fetchall()
    except Exception as e:
        print('접속오류', e)
    finally:
        return rows

async def get_hotcoins(request, db):
    try:
        query = text("SELECT * FROM orderbookAmt where dateTag = (select max(dateTag) from orderbookAmt)")
        result = await db.execute(query)
        orderbooks = result.fetchall()
        return orderbooks
    except Exception as e:
        print("Error!!", e)
        return False


async def get_hotamt(request, db):
    try:
        query = text("select * from tradeAmt order by regDate desc limit 1")
        result = await db.execute(query)
        hotamt = result.fetchone()
        return hotamt
    except Exception as e:
        print("Error!!", e)
        return False


async def detailuser(uno:int, db: AsyncSession = Depends(get_db)):
    row = None
    try:
        sql = text("SELECT * FROM traceUser WHERE userNo = :userno and attrib NOT LIKE :xattr")
        result = await db.execute(sql, {"userno":uno,"xattr": "%XXXUP%"})
        row = result.fetchone()
    except Exception as e:
        print('접속오류', e)
    finally:
        return row


async def get_onoff(uno:int, db: AsyncSession = Depends(get_db)):
    row = None
    try:
        sql = text("SELECT activeYN FROM mtSetup WHERE userNo = :userno and attrib NOT LIKE :xattr")
        result = await db.execute(sql, {"userno":uno,"xattr": "%XXX%"})
        row = result.fetchone()
    except Exception as e:
        print('접속오류', e)
    finally:
        return row

async def setKeys(uno:int, setkey:str, db: AsyncSession = Depends(get_db)):
    try:
        sql = text("UPDATE traceUser SET setupKey = :setk, lastLogin = now() where userNo=:userno")
        await db.execute(sql, {"userno":uno,"setk":setkey})
        await db.commit()
    except Exception as e:
        print('접속오류', e)
    finally:
        return True

async def check_setkey(uno:int,setkey:str, db: AsyncSession = Depends(get_db)):
    try:
        sql = text("SELECT userNo FROM traceUser WHERE userNo=:userno and setupKey=:setkey and attrib not like :xattr")
        result = await db.execute(sql, {"userno":uno,"setkey":setkey,"xattr": "%XXXUP%"})
        row = result.fetchone()
        if row[0] == uno:
            return True
        else:
            return False
    except Exception as e:
        print("코드 체크 에러",e)

async def get_userdetail(uno:int,setkey:str, db: AsyncSession = Depends(get_db)):
    try:
        sql = text("SELECT * FROM traceUser WHERE userNo=:userno and setupKey=:setkey and attrib not like :xattr")
        result = await db.execute(sql, {"userno":uno,"setkey":setkey,"xattr": "%XXXUP%"})
        row = result.fetchone()
        if row[0] == uno:
            return row
        else:
            return False
    except Exception as e:
        print("유저정보 취득 에러",e)

def require_login(request: Request):
    user_no = request.session.get("user_No")
    if not user_no:
        raise HTTPException(
            status_code=status.HTTP_303_SEE_OTHER,
            headers={"Location": "/"},
            detail="세션이 만료되어 재로그인이 필요합니다."
        )
    return user_no

async def getKeys(uno:int,setkey:str, db: AsyncSession = Depends(get_db)):
    global key1, key2
    key1 = None
    key2 = None
    try:
        sql = text("SELECT apiKey1, apiKey2 FROM traceUser WHERE setupKey=:setk AND userNo=:userno and attrib not like :xattr")
        result = await db.execute(sql, {"setk":setkey,"userno":uno,"xattr": "%XXXUP%"})
        keys = result.fetchone()
        if len(keys) == 0:
            print("No available Keys !!")
        else:
            key1 = keys[0]
            key2 = keys[1]
    except Exception as e:
        print('키로드 오류')
        return False
    finally:
        return key1, key2

async def api_getKeys(uno:int,db: AsyncSession = Depends(get_db)):
    global key1, key2
    key1 = None
    key2 = None
    try:
        sql = text("SELECT apiKey1, apiKey2 FROM traceUser WHERE userNo=:userno and attrib not like :xattr")
        result = await db.execute(sql, {"userno":uno,"xattr": "%XXXUP%"})
        keys = result.fetchone()
        if len(keys) == 0:
            print("No available Keys !!")
        else:
            key1 = keys[0]
            key2 = keys[1]
    except Exception as e:
        print('키로드 오류')
        return False
    finally:
        return key1, key2

async def checkwallet(uno:int, setkey:str, db: AsyncSession = Depends(get_db)):
    global key1, key2, walletitems
    walletitems = []
    try:
        keys = await getKeys(uno,setkey,db)
        key1 = keys[0]
        key2 = keys[1]
        upbit = pyupbit.Upbit(key1, key2)
        walletitems = upbit.get_balances()
    except Exception as e:
        print("지갑 불러오기 에러", e)
        return False
    finally:
        return walletitems


async def api_checkwallet(uno:int, db: AsyncSession = Depends(get_db)):
    global key1, key2, walletitems
    walletitems = []
    try:
        keys = await api_getKeys(uno,db)
        key1 = keys[0]
        key2 = keys[1]
        upbit = pyupbit.Upbit(key1, key2)
        walletitems = upbit.get_balances()
    except Exception as e:
        print("지갑 불러오기 에러", e)
        return False
    finally:
        return walletitems

async def clearcache(db: AsyncSession = Depends(get_db)):
    try:
        sql = text("RESET QUERY CACHE")
        await db.execute(sql)
        await db.commit()
    except Exception as e:
        print("Clear Cache Error !!",e)
        return False
    finally:
        return True

async def buycoinmarket(uno, coink, setkey, amt, db: AsyncSession = Depends(get_db)):
    try:
        keys = await getKeys(uno,setkey,db)
        key1 = keys[0]
        key2 = keys[1]
        upbit = pyupbit.Upbit(key1, key2)
        buy = upbit.buy_market_order(coink, amt)
        return buy
    except Exception as e:
        print("시장가 구매 에러",e)
        return False

async def api_buycoinmarket(uno, coink, amt, db: AsyncSession = Depends(get_db)):
    try:
        keys = await api_getKeys(uno,db)
        key1 = keys[0]
        key2 = keys[1]
        upbit = pyupbit.Upbit(key1, key2)
        buy = upbit.buy_market_order(coink, amt)
        return buy
    except Exception as e:
        print("시장가 구매 에러",e)
        return False

async def sellcoinpercent(uno, coink, setkey, volm, db: AsyncSession = Depends(get_db)):
    try:
        keys = await getKeys(uno, setkey, db)
        key1 = keys[0]
        key2 = keys[1]
        upbit = pyupbit.Upbit(key1, key2)
        walt = upbit.get_balances() #지갑내 잔고 확보
        crp = pyupbit.get_current_price(coink) #현재가 재확인
        for coin in walt: #잔고중 일치하는 코인만 잔고
            if coin['currency'] == coink.split('-')[1]:
                if float(coin['balance'])*float(crp) < 5000:
                    buy5000 = upbit.buy_market_order(coink, 5000) #5000원 추가구매
                    break
                else:
                    result = upbit.sell_market_order(coink, volm)
                    return result
            else:
                continue
    except Exception as e:
        print("시장가 비율 매도 에러",e)
        return False

async def api_sellcoinpercent(uno, coink, volm, db: AsyncSession = Depends(get_db)):
    try:
        keys = await api_getKeys(uno, db)
        key1 = keys[0]
        key2 = keys[1]
        upbit = pyupbit.Upbit(key1, key2)
        walt = upbit.get_balances() #지갑내 잔고 확보
        crp = pyupbit.get_current_price(coink) #현재가 재확인
        for coin in walt: #잔고중 일치하는 코인만 잔고
            if coin['currency'] == coink.split('-')[1]:
                if float(coin['balance'])*float(crp) < 5000:
                    buy5000 = upbit.buy_market_order(coink, 5000) #5000원 추가구매
                    break
                else:
                    result = upbit.sell_market_order(coink, volm)
                    return result
            else:
                continue
    except Exception as e:
        print("시장가 비율 매도 에러",e)
        return False

async def cancelorder(uno:int,setkey:str ,uuid:str, db: AsyncSession = Depends(get_db)):
    global rows
    try:
        keys = await getKeys(uno,setkey,db)
        upbit = await pyupbit.Upbit(keys[0], keys[1])
        await upbit.cancel_order(uuid)
        return True
    except Exception as e:
        print("거래 취소 에러 ", e)
        return False

async def api_cancelorder(uno:int,uuid:str, db: AsyncSession = Depends(get_db)):
    global rows
    try:
        keys = await api_getKeys(uno,db)
        upbit = await pyupbit.Upbit(keys[0], keys[1])
        await upbit.cancel_order(uuid)
        return True
    except Exception as e:
        print("거래 취소 에러 ", e)
        return False


async def setmypasswd(uno:int,setkey:str, passwd:str, db: AsyncSession = Depends(get_db)):
    try:
        sql = text("UPDATE traceUser SET userPasswd = password(:pass) where userNo=:userno")
        await db.execute(sql, {"userno":uno,"pass":passwd})
        await db.commit()
        return True
    except Exception as e:
        print('비밀번호 업데이트 오류', e)

async def dashcandle548(coink):
    candles: DataFrame | None = await pyupbit.get_ohlcv(coink, interval="minute5", count=48)
    return candles

async def tradedcoins(uno:int, db: AsyncSession = Depends(get_db)):
    global coins
    try:
        sql = text("select distinct bidCoin from traceSetup where userNo=:userno and attrib not like :xattr order by bidCoin asc")
        rows = await db.execute(sql,{"userno":uno,"xattr":"%XXXUP%"} )
        coins = rows.fetchall()
        coins = [list(coins[x]) for x in range(len(coins))]
    except Exception as e:
        print("거래 코인 목록 조회 에러 ", e)
    finally:
        return coins

async def get_tradelogupbit(coink:str, userno:int, setkey:str, db: AsyncSession = Depends(get_db)):
    global rows
    try:
        keys = await getKeys(userno,setkey,db)
        upbit = pyupbit.Upbit(keys[0], keys[1])
        rows = upbit.get_order(coink, state="done")
        return rows
    except Exception as e:
        print("거래이력 불러오기 에러",e)
        return False

async def api_tradelogupbit(coink:str, userno:int, db: AsyncSession = Depends(get_db)):
    global rows
    try:
        keys = await api_getKeys(userno,db)
        upbit = pyupbit.Upbit(keys[0], keys[1])
        rows = upbit.get_order(coink, state="done")
        return rows
    except Exception as e:
        print("거래이력 불러오기 에러",e)
        return False

async def get_orderlist(userno:int, setkey:str,slot:int, db: AsyncSession = Depends(get_db)):
    global rows
    try:
        keys = await getKeys(userno,setkey,db)
        upbit = pyupbit.Upbit(keys[0], keys[1])
        setups = await (userno, slot, db)
        orders = []
        for setup in setups:
            coink = setup[6]
            order = upbit.get_order(coink)
            orders.extend(order)
        return orders
    except Exception as e:
        print("주문내용 불러오기 에러",e)
        return False


async def get_mtorderlist(userno:int, setkey:str,db: AsyncSession = Depends(get_db)):
    global rows
    try:
        keys = await getKeys(userno,setkey,db)
        upbit = pyupbit.Upbit(keys[0], keys[1])
        setups = await checkwallet(userno, setkey, db)
        orders = []
        for setup in setups:
            if setup["currency"] != "KRW":
                coink = "KRW-" + setup["currency"]
                order = upbit.get_order(coink)
                orders.extend(order)
        return orders
    except Exception as e:
        print("mt주문내용 불러오기 에러",e)
        return False

async def api_mtorderlist(userno:int, db: AsyncSession = Depends(get_db)):
    global rows
    try:
        keys = await api_getKeys(userno,db)
        upbit = pyupbit.Upbit(keys[0], keys[1])
        setups = await api_checkwallet(userno, db)
        orders = []
        for setup in setups:
            if setup["currency"] != "KRW":
                coink = "KRW-" + setup["currency"]
                order = upbit.get_order(coink)
                orders.extend(order)
        return orders
    except Exception as e:
        print("mt주문내용 불러오기 에러",e)
        return False

async def getsetups(uno:int, slotno:int, db: AsyncSession = Depends(get_db)):
    try:
        if slotno == 0:
            sql = text("select * from traceSetup where userNo=:userno and attrib not like :xatts")
            rows = await db.execute(sql, {"userno":uno, "xatts":'%XXXUP%'})
            setups = list(rows.fetchall())
            return setups
        else:
            sql = text("select * from traceSetup where userNo=:userno and slot = :slot and attrib not like :xattr")
            rows = await db.execute(sql, {"userno":uno,"slot":slotno,"xattr":'%XXXUP%'} )
            setups = list(rows.fetchall())
            return setups
    except Exception as e:
        print('설정 불러오기 오류', e)
        return False


async def getmtpondsetups(uno: int, slotno: int, db: AsyncSession = Depends(get_db)):
    try:
        if slotno == 0:
            sql = text("select * from mtPondSetup where userNo=:userno and attrib not like :xatts")
            rows = await db.execute(sql, {"userno": uno, "xatts": '%XXXUP%'})
            setups = list(rows.fetchall())
            return setups
        else:
            # slotno에 따라 3개의 번호 생성
            start_slot = (slotno - 1) * 3 + 1
            slotnos = [start_slot, start_slot + 1, start_slot + 2]
            sql = text("select * from mtPondSetup where userNo=:userno and slotNo in :slotnos and attrib not like :xattr")
            rows = await db.execute(sql, {"userno": uno, "slotnos": tuple(slotnos), "xattr": '%XXXUP%'})
            setups = list(rows.fetchall())
            return setups
    except Exception as e:
        print('설정 불러오기 오류', e)
        return False


async def get_mtsetups(uno: int, db: AsyncSession = Depends(get_db)):
    try:
        sql = text("select * from mtSetup where userNo=:userno and attrib not like :xatts")
        rows = await db.execute(sql, {"userno": uno, "xatts": '%XXXUP%'})
        setups = list(rows.fetchall())
        return setups
    except Exception as e:
        print('설정 불러오기 오류', e)
        return False

async def api_mtsetups(uno: int, db: AsyncSession = Depends(get_db)):
    try:
        sql = text("select * from mtSetup where userNo=:userno and attrib not like :xatts")
        rows = await db.execute(sql, {"userno": uno, "xatts": '%XXXUP%'})
        setups = list(rows.fetchall())
        return setups
    except Exception as e:
        print('설정 불러오기 오류', e)
        return False


async def setonoffs(setno:int, yesno:str, db: AsyncSession = Depends(get_db)):
    try:
        sql = text("UPDATE mtPondSetup SET activeYN = :yesno where setupNo=:setno AND attrib not like :xattr")
        await db.execute(sql, {"setno":setno, "yesno":yesno, "xattr":'%XXXUP%'})
        await db.commit()
    except Exception as e:
        print('거래 ON/OFF 오류', e)

async def setonoff(uno:int, yesno:str, db: AsyncSession = Depends(get_db)):
    try:
        sql = text("UPDATE mtSetup SET activeYN = :yesno where userNo=:userno AND attrib not like :xattr")
        await db.execute(sql, {"userno":uno, "yesno":yesno, "xattr":'%XXXUP%'})
        await db.commit()
    except Exception as e:
        print('거래 ON/OFF 오류', e)

async def get_trsetups(uno, db: AsyncSession = Depends(get_db)):
    try:
        query = text("SELECT * FROM polarisSets where userNo = :uno and attrib not like :attxx")
        result = await db.execute(query, {"uno": uno, "attxx": "%XXX%"})
        mysetups = result.fetchall()
        mysets = []
        for setup in mysetups:
            mysets.append({
                "setupNo": setup[0],
                "coinName": setup[2],
                "stepAmt": setup[3],
                "tradeType": setup[4],
                "maxAmt": setup[5],
                "useYN": setup[6],
            })
    except Exception as e:
        print("Get Setup Error!!", e)
        raise HTTPException(status_code=500, detail="Internal Server Error")
    finally:
        return mysets

async def setautostop(sno:int, yesno:str, db: AsyncSession = Depends(get_db)):
    try:
        sql = text("UPDATE traceSetup SET doubleYN = :yesno where setupNo=:sno")
        await db.execute(sql, {"sno":sno, "yesno":yesno})
        await db.commit()
    except Exception as e:
        print('자동 멈춤 기능 설정 오류', e)

async def setlconoff(setno:int, lcrate:float, yesno:str, db: AsyncSession = Depends(get_db)):
    try:
        sql = text("UPDATE mtPondSetup SET losscut = :lcrate, lcYN = :yesno where setupNo=:setno")
        await db.execute(sql, {"lcrate":lcrate, "yesno":yesno, "setno":setno})
        await db.commit()
    except Exception as e:
        print('손절 ONOFF 오류', e)

async def selectsetlist(db: AsyncSession = Depends(get_db)):
    try:
        sql = text("SELECT * FROM traceSets WHERE useYN = :useyn and attrib NOT LIKE :xattr")
        rows = await db.execute(sql, {"useyn":"Y","xattr":"%XXXUP%"} )
        sets = rows.fetchall()
        return sets
    except Exception as e:
        print('트레이딩 설정 목록 불러오기 오류', e)

async def erasebid(uno:int, setkey:str, tabindex:int, db: AsyncSession = Depends(get_db)):
    try:
        sql2 = text("update traceSetup set attrib=:xattr where userNo=:userno and slot = :slot")
        await db.execute(sql2, {"xattr":"XXXUPXXXUPXXXUP", "userno":uno,"slot": tabindex})
        await db.commit()
        return True
    except Exception as e:
        return False

async def erasemtpondsetup(uno:int, setkey:str, db: AsyncSession = Depends(get_db)):
    try:
        sql1 = text("update mtSetup set attrib=:xattr where userNo=:userno")
        await db.execute(sql1, {"xattr": "XXXUPXXXUP", "userno": uno})
        await db.commit()
        return True
    except Exception as e:
        return False

async def erasemtpondsetup_backup(uno:int, setkey:str, slot:int, db: AsyncSession = Depends(get_db)):
    try:
        start_slot = (slot - 1) * 3 + 1
        for tabindex in range(start_slot, start_slot + 3):
            sql2 = text("update mtPondSetup set attrib=:xattr where userNo=:userno and slotNo = :slot")
            await db.execute(sql2, {"xattr": "XXXUPXXXUP", "userno": uno, "slot": tabindex})
        await db.commit()
        return True
    except Exception as e:
        return False

async def update_userdtl(uno:int, key1:str , key2:str, svrno:int ,db: AsyncSession = Depends(get_db)):
    try:
        sql2 = text("update traceUser set apiKey1 = :key1 , apiKey2 = :key2, serverNo = :svrno where userNo=:userno")
        await db.execute(sql2, {"key1": key1, "key2": key2, "svrno":svrno ,"userno":uno})
        await db.commit()
        return True
    except Exception as e:
        return False

async def setupbid(uno:int, setkey:str, initbid:float, bidstep:int, bidrate:float, askrate:float, coinn:str, svrno:int, tradeset:int, holdNo:int, doubleYN:str, limitamt:float,limityn:str, slot:int, db: AsyncSession = Depends(get_db)):
    chkkey = await check_setkey(uno, setkey, db)
    if chkkey is True:
        try:
            sql = text("""
                insert into traceSetup 
                (userNo, initAsset, bidInterval, bidRate, askrate, bidCoin, custKey, serverNo, holdNo, doubleYN, limitAmt, limitYN, slot, regDate)
                VALUES (:uno, :initbid, :bidstep, :bidrate, :askrate, :coinn, :tradeset, :svrno, :holdNo, :doubleYN, :limitamt, :limityn, :slot, now())
            """)
            await db.execute(sql, {
                "uno": uno,"initbid": initbid,"bidstep": bidstep,
                "bidrate": bidrate,"askrate": askrate,"coinn": coinn,
                "tradeset": tradeset,"svrno": svrno,"holdNo": holdNo,
                "doubleYN": doubleYN,"limitamt": limitamt,"limityn": limityn, "slot": slot,
            })
            await db.commit()
            return True
        except Exception as e:
            print('트레이딩 설정 저장 오류', e)
            return False
    else:
        return False

async def setupmymtpondset(uno:int, setkey:str, initbid:float, addbid:float, limitbid:float, minmargin:float, cutrate:float,  db: AsyncSession = Depends(get_db)):
    chkkey = await check_setkey(uno, setkey, db)
    if chkkey is True:
        try:
            sql = text("""
                insert into mtSetup (userNo,initAmt,addAmt,limitAmt,minMargin,lcRate)
                VALUES (:userNo, :iniBid , :addBid, :limitBid, :minMargin, :losscut)
            """)
            await db.execute(sql, {
                "userNo": uno,"iniBid": initbid,"addBid": addbid,"limitBid": limitbid, "minMargin":minmargin, "losscut":cutrate})
            await db.commit()
            return True
        except Exception as e:
            print('mtPond 트레이딩 설정 저장 오류', e)
            return False
    else:
        return False

async def setupmymtpondset_backup(uno:int, setkey:str, slot:int, initbid:float, addbid:float, limitbid:float, minmargin:float, cutrate:float, lcyn:str, svrno:int,  db: AsyncSession = Depends(get_db)):
    chkkey = await check_setkey(uno, setkey, db)
    if chkkey is True:
        try:
            sql = text("""
                insert into mtPondSetup (userNo,slotNo,iniBid,addBid,limitBid,minMargin,losscut,lcYN,serverNo)
                VALUES (:userNo, :slotNo, :iniBid , :addBid, :limitBid, :minMargin, :losscut, :lcYN, :serverNo)
            """)
            await db.execute(sql, {
                "userNo": uno,"slotNo": slot,"iniBid": initbid,"addBid": addbid,"limitBid": limitbid, "minMargin":minmargin, "losscut":cutrate, "lcYN":lcyn, "serverNo":svrno
            })
            await db.commit()
            return True
        except Exception as e:
            print('mtPond 트레이딩 설정 저장 오류', e)
            return False
    else:
        return False

async def editbidsetup( sno:int, uno:int, setkey:str, initbid:float, bidstep:int, bidrate:float, askrate:float, coinn:str, svrno:int, tradeset:int, holdNo:int, doubleYN:str, limitamt:float,limityn:str, slot:int, db: AsyncSession = Depends(get_db)):
    chkkey = await check_setkey(uno, setkey, db)
    if chkkey == True:
        try:
            sqlp = text("update traceSetup set attrib=:xattr where setupNo=:sno")
            await db.execute(sqlp, {"sno":sno,"xattr":"XXXUPXXXUPXXXUP"})
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
    else:
        return False
@app.get("/")
async def root(request: Request):
    return templates.TemplateResponse("/login/login.html", {"request": request})

@app.post("/loginchk")
async def login(request: Request, response: Response, uid: str = Form(...), upw: str = Form(...),
                db: AsyncSession = Depends(get_db)):
    user =await selectUsers(uid, upw, db)
    print(user)
    if user is None:
        return templates.TemplateResponse("login/login.html", {"request": request, "error": "Invalid credentials"})
    else:
        await setKeys(user[0][0], user[1], db)
    # 서버 세션에 사용자 ID 저장
    request.session["user_No"] = user[0][0]
    request.session["user_Name"] = user[0][1]
    request.session["server_No"] = user[0][2]
    request.session["user_Role"] = user[0][3]
    request.session["License"] = user[0][4]
    request.session["setKey"] = user[1]
    return RedirectResponse(url=f"/upbittop30/{user[0][0]}/{user[1]}", status_code=303)

@app.get("/logout")
async def logout(request: Request):
    request.session.clear()  # 세션 삭제
    return RedirectResponse(url="/")


@app.get("/hotcoin_list/{uno}")
async def hotcoinlist(request: Request, uno: int, user_session: int = Depends(require_login),
                      db: AsyncSession = Depends(get_db)):
    orderbooks = None
    if uno != user_session:
        return RedirectResponse(url="/", status_code=303)
    usern = request.session.get("user_Name")
    setkey = request.session.get("setupKey")
    try:
        orderbooks = await get_hotcoins(request, db)
        hotamt = await get_hotamt(request, db)
        gettime = orderbooks[0][8]
        nowtt = datetime.now()
        diff = nowtt - gettime
        days = diff.days
        hours = diff.seconds // 3600
        minutes = (diff.seconds % 3600) // 60
        seconds = diff.seconds % 60
        time_diff = f"{days}일 {hours}시간 {minutes}분 {seconds}초 "
        is_reloadable = "Y" if diff.total_seconds() > 10800 else "N" # 3시간
        trsetups = await get_trsetups(uno, db)
        return templates.TemplateResponse(
            "/trade/hotcoinlist.html",
            {
                "request": request,
                "userNo": uno,
                "user_No": uno,
                "userName": usern,
                "setkey": setkey,
                "orderbooks": orderbooks,
                "time_diff": time_diff,
                "trsetups": trsetups,
                "reloadable": is_reloadable,
                "hotamt": hotamt,
            }
        )
    except Exception as e:
        print("Get Hotcoins Error !!", e)


@app.get("/balance/{uno}")
async def my_balance(request: Request, uno: int, user_session: int = Depends(require_login),
                     db: AsyncSession = Depends(get_db)):
    global myavgp
    mycoins = None
    myavgp = None
    if uno != user_session:
        return RedirectResponse(url="/", status_code=303)
    try:
        setKey = request.session.get("setKey")
        userName = request.session.get("user_Name")
        userRole = request.session.get("user_Role")
        userLicense = request.session.get("License")
        mycoins = await checkwallet(uno,setKey, db)
        cprices = await get_current_prices()
    except Exception as e:
        print("Get Balances Error !!", e)
        mycoins = None
    return templates.TemplateResponse("wallet/mywallet.html",
                                      {"request": request, "user_No": uno,"user_Name":userName, "user_Role":userRole, "setkey":setKey,"license":userLicense ,"mycoins": mycoins, "myavgp":myavgp, "cuprices":cprices})



@app.post("/tradebuymarket/{uno}/{setkey}/{coinn}/{costk}")
async def tradebuymarket(request: Request,uno: int,setkey:str, coinn: str, costk: float, user_session: int = Depends(require_login), db: AsyncSession = Depends(get_db)):
    if uno != user_session:
        return JSONResponse({"success": False, "message": "권한이 없습니다.", "redirect": "/"})
    else:
        ssesskey = request.session.get("setKey")
        if int(setkey) != int(ssesskey):
            return JSONResponse({"success": False, "message": "권한이 없습니다.", "redirect": "/"})
    try:
        coink = "KRW-" + coinn
        butm = await buycoinmarket(uno, coink, setkey, costk,db)
        if butm:
            # 거래 성공
            return JSONResponse({"success": True, "redirect": f"/balance/{uno}"})
        else:
            # 거래 실패
            return JSONResponse({"success": False, "message": "거래 실패", "redirect": f"/balance/{uno}"})
    except Exception as e:
        print("Error!!", e)
        return JSONResponse({"success": False, "message": "서버 오류", "redirect": f"/balance/{uno}"})

@app.post("/tradesellmarket/{uno}/{setkey}/{coinn}/{volm}")
async def tradesellmarket(request: Request,uno: int,setkey:str, coinn: str, volm: float, user_session: int = Depends(require_login), db: AsyncSession = Depends(get_db)):
    if uno != user_session:
        return JSONResponse({"success": False, "message": "권한이 없습니다.", "redirect": "/"})
    else:
        ssesskey = request.session.get("setKey")
        if int(setkey) != int(ssesskey):
            return JSONResponse({"success": False, "message": "권한이 없습니다.", "redirect": "/"})
    try:
        coink = "KRW-" + coinn
        sellm = await sellcoinpercent(uno, coink, setkey, volm ,db)
        if sellm:
            # 거래 성공
            return JSONResponse({"success": True, "redirect": f"/balance/{uno}"})
        else:
            # 거래 실패
            return JSONResponse({"success": False, "message": "거래 실패", "redirect": f"/balance/{uno}"})
    except Exception as e:
        print("Error!!", e)
        return JSONResponse({"success": False, "message": "서버 오류", "redirect": f"/balance/{uno}"})

@app.get('/tradedetail/{userno}/{setkey}')
async def tradedetail(request:Request ,userno:int,setkey:str, db: AsyncSession = Depends(get_db)):
    coinlist = pyupbit.get_tickers(fiat="KRW")
    trcoins = await tradedcoins(userno, db)
    trlogs = []
    dates = []
    userName = request.session.get("user_Name")
    userRole = request.session.get("user_Role")
    return templates.TemplateResponse('./trade/mytradingresult.html', {"request": request, "coinlist": coinlist, "trcoins": trcoins, "user_No":userno,"user_Name":userName, "user_Role":userRole ,"setkey":setkey, "reqitems":trlogs, "dates":dates})

@app.get('/tradedetails/{userno}/{setkey}/{coink}')
async def tradedetails(request:Request ,userno:int,setkey:str,coink:str ,db: AsyncSession = Depends(get_db)):
    coinlist = pyupbit.get_tickers(fiat="KRW")
    trcoins = await tradedcoins(userno, db)
    trlogs = await get_tradelogupbit(coink, userno, setkey, db)
    dates = list({item['created_at'][:10] for item in trlogs})
    dates.sort(reverse=True)  # 최신순 정렬
    userName = request.session.get("user_Name")
    userRole = request.session.get("user_Role")
    return templates.TemplateResponse('./trade/mytradingresult.html', {"request": request, "coinlist": coinlist, "trcoins": trcoins, "user_No":userno,"user_Name":userName, "user_Role":userRole , "setkey":setkey, "reqitems":trlogs, "dates":dates, "coink":coink })

@app.get('/tradetrend/{userno}/{setkey}')
async def tradetrend(request:Request ,userno:int,setkey:str, db: AsyncSession = Depends(get_db)):
    trcoins = await tradedcoins(userno, db)
    mycoins = await checkwallet(userno, setkey, db)
    userName = request.session.get("user_Name")
    userRole = request.session.get("user_Role")
    return templates.TemplateResponse('./trade/mytradingtrend.html', {"request": request,"trcoins": trcoins, "user_No":userno,"user_Name":userName, "user_Role":userRole ,"setkey":setkey, "mycoins" :mycoins})


@app.get('/upbittradetrend/{userno}/{setkey}')
async def upbittradetrend(request:Request ,userno:int,setkey:str, db: AsyncSession = Depends(get_db)):
    trcoins = []
    hotcoins = await get_hotcoins(request, db)
    trcoins = [row[3] for row in hotcoins]
    userName = request.session.get("user_Name")
    userRole = request.session.get("user_Role")
    return templates.TemplateResponse('./trade/upbittradingtrend.html', {"request": request,"trcoins": trcoins, "user_No":userno,"user_Name":userName, "user_Role":userRole ,"setkey":setkey})

@app.get('/settletrend/{userno}/{setkey}')
async def tradetrend(request:Request ,userno:int,setkey:str, db: AsyncSession = Depends(get_db)):
    trcoins = await tradedcoins(userno, db)
    mycoins = await checkwallet(userno, setkey, db)
    userName = request.session.get("user_Name")
    userRole = request.session.get("user_Role")
    return templates.TemplateResponse('./trade/mysettletrend.html', {"request": request,"trcoins": trcoins, "user_No":userno,"user_Name":userName, "user_Role":userRole ,"setkey":setkey, "mycoins" :mycoins})

@app.get('/upbitsettletrend/{userno}/{setkey}')
async def upbittradetrend(request:Request ,userno:int,setkey:str, db: AsyncSession = Depends(get_db)):
    trcoins = []
    hotcoins = await get_hotcoins(request, db)
    trcoins = [row[3] for row in hotcoins]
    userName = request.session.get("user_Name")
    userRole = request.session.get("user_Role")
    return templates.TemplateResponse('./trade/upbitsettletrend.html', {"request": request,"trcoins": trcoins, "user_No":userno,"user_Name":userName, "user_Role":userRole ,"setkey":setkey })

@app.get('/userEdit/{userno}/{setkey}')
async def useredit(request:Request ,userno:int,setkey:str, db: AsyncSession = Depends(get_db)):
    userName = request.session.get("user_Name")
    userRole = request.session.get("user_Role")
    userdtl = await get_userdetail(userno,setkey,db)
    print(userdtl)
    return templates.TemplateResponse('./login/userDtl.html', {"request": request,"user_No":userno,"user_Name":userName, "user_Role":userRole ,"setkey":setkey, "userdtl":userdtl})

@app.get('/rest_getwallet/{userno}/{setkey}')
async def restgetwallet(request:Request ,userno:int,setkey:str,db: AsyncSession = Depends(get_db)):
    try:
        mycoins = await checkwallet(userno, setkey, db)
        return JSONResponse({"success": True, "data": mycoins})
    except Exception as e:
        print("Error!!", e)
        return JSONResponse({"success": False, "data": [] })

@app.get('/mytradestat/{userno}/{setkey}/{slot}')
async def mytradestat(request:Request ,userno:int,setkey:str,slot:int,user_session: int = Depends(require_login), db: AsyncSession = Depends(get_db)):
    try:
        setups = await getsetups(userno, slot, db)
        userName = request.session.get("user_Name")
        userRole = request.session.get("user_Role")
        userLicense = request.session.get("License")
        mycoins = await checkwallet(userno, setkey, db)
        orderlist = await get_orderlist(userno, setkey, slot, db)
        return templates.TemplateResponse('/trade/mytrademain.html', {"request":request, "setups":setups, "user_No":userno,"user_Name":userName, "user_Role":userRole ,"setkey":setkey,"license":userLicense,"mycoins" :mycoins, "slot":slot, "orderlist":orderlist })
    except Exception as e:
        print("트레이딩 상태 불러오기 에러",e)


@app.get('/mymtpondstat/{userno}/{setkey}')
async def mymtpondstat(request:Request ,userno:int,setkey:str,user_session: int = Depends(require_login), db: AsyncSession = Depends(get_db)):
    try:
        userName = request.session.get("user_Name")
        userRole = request.session.get("user_Role")
        userLicense = request.session.get("License")
        onoffstat = await get_onoff(userno, db)
        mysettings = await get_mtsetups(userno,db)
        mycoins = await checkwallet(userno, setkey, db)
        myorders = await get_mtorderlist(userno,setkey ,db)
        return templates.TemplateResponse('/trade/mypondmain.html', {"request":request, "user_No":userno,"user_Name":userName, "user_Role":userRole ,"setkey":setkey,"license":userLicense, "onoffstat":onoffstat[0], "mysettings":mysettings, "myorders":myorders, "mycoins" :mycoins })
    except Exception as e:
        print("mtPond 트레이딩 상태 불러오기 에러",e)


@app.get('/mytradeSet/{userno}')
async def mytradeSet(request:Request,userno:int,db: AsyncSession = Depends(get_db)):
    coinlist = pyupbit.get_tickers(fiat="KRW")
    userName = request.session.get("user_Name")
    userRole = request.session.get("user_Role")
    setkey = request.session.get("setKey")
    trcnt = request.session.get("License")
    serverno = request.session.get("server_No")
    setlist = await selectsetlist(db)
    return templates.TemplateResponse('/trade/setmytrades.html', {"request":request,"coinlist":coinlist, "setlist":setlist, "trcnt":trcnt,"user_Name":userName,"setkey":setkey,"user_No":userno, "user_Role":userRole, "server_No":serverno })


@app.get('/mymtpondSet/{userno}')
async def mypondSet(request:Request,userno:int,db: AsyncSession = Depends(get_db)):
    coinlist = pyupbit.get_tickers(fiat="KRW")
    userName = request.session.get("user_Name")
    userRole = request.session.get("user_Role")
    setkey = request.session.get("setKey")
    trcnt = request.session.get("License")
    serverno = request.session.get("server_No")
    return templates.TemplateResponse('/trade/setmtpond.html', {"request":request,"coinlist":coinlist, "trcnt":trcnt,"user_Name":userName,"setkey":setkey,"user_No":userno, "user_Role":userRole, "server_No":serverno })


@app.get('/editSetup')
async def editSetup(request:Request,
    setno: str = Query(...),
    coinA: str = Query(...),
    coinB: str = Query(...),
    tabindex: str = Query(...),
    db: AsyncSession = Depends(get_db)
):
    coinlist = pyupbit.get_tickers(fiat="KRW")
    setlist = await selectsetlist(db)
    setkey = request.session.get("setKey")
    userno = request.session.get("user_No")
    serverno = request.session.get("server_No")
    usernameno = request.session.get("user_Name")
    userrole = request.session.get("user_Role")
    return templates.TemplateResponse(
        './trade/editmytrade.html',
        {
            "request": request,
            "coinlist": coinlist,
            "setno": setno,
            "coinA": coinA,
            "coinB": coinB,
            "setlist": setlist,
            "tabindex": tabindex,
            "setkey": setkey,
            "user_No": userno,
            "user_Name": usernameno,
            "user_Role": userrole,
            "server_No": serverno,
        }
    )

@app.post("/setupbids")
async def setupmybids(
    userno: str = Form(...),
    tabindex: str = Form(...),
    initprice: str = Form(...),
    lcrate: Optional[str] = Form(None),
    lcchk: Optional[str] = Form(None),
    tradeset: str = Form(...),
    coinn1: Optional[str] = Form(None),
    coinn2: Optional[str] = Form(None),
    coinn3: Optional[str] = Form(None),
    setkey: str = Form(...),
    svrno: str = Form(...),
    limityn: Optional[str] = Form(None),
    limitamt: Optional[str] = Form(None),
        db: AsyncSession = Depends(get_db),
):
    uno = int(userno)
    slot = int(tabindex)
    price = initprice.replace(',', '') if initprice else ''
    askrate = lcrate
    lcchk_val = lcchk
    tradeset_split = tradeset.split(',')
    tradeset_val = tradeset_split[0]
    bidsetps = tradeset_split[1] if len(tradeset_split) > 1 else None
    hno = tradeset_split[1] if len(tradeset_split) > 1 else None
    dyn = 'Y' if limityn == 'on' else 'N'
    limityn_value = 'Y' if limityn == 'on' else 'N'
    lmtamt = (limitamt or '').replace(',', '') if limitamt else ''
    bidrate = 1.0 if lcchk_val == 'on' else 0.0
    await erasebid(uno, setkey, slot, db)
    for coin in [coinn1, coinn2, coinn3]:
        if coin:
            await setupbid(
                uno, setkey, price, bidsetps, bidrate, askrate, coin, svrno,
                tradeset_val, hno, dyn, lmtamt, limityn_value, slot, db
            )
    return RedirectResponse(url=f"/mytradestat/{uno}/{setkey}/{slot}", status_code=303)


@app.post("/setupmtponds")
async def setupmtponds(
    userno: str = Form(...),
    initprice: str = Form(...),
    addprice: str = Form(...),
    limitamt: str = Form(...),
    minmargin: str = Form(...),
    lcrate: Optional[str] = Form(None),
    setkey: str = Form(...),
        db: AsyncSession = Depends(get_db),
):
    uno = int(userno)
    initprice = initprice.replace(',', '') if initprice else '0'
    addprice = addprice.replace(',', '') if addprice else '0'
    limitamt = limitamt.replace(',','') if limitamt else '0'
    lcrate = lcrate or '0'
    minmargin = minmargin.replace(',', '') if minmargin else ''
    await erasemtpondsetup(uno, setkey,db)
    await setupmymtpondset(uno, setkey, initprice, addprice, limitamt, minmargin, lcrate,db)
    return RedirectResponse(url=f"/mymtpondstat/{uno}/{setkey}", status_code=303)

@app.post("/setupbid")
async def setupmybid(
    setno: str = Form(...),
    userno: str = Form(...),
    slot: str = Form(...),
    coinn: str = Form(...),
    initprice: str = Form(...),
    lcrate: Optional[str] = Form(None),
    lcchk: Optional[str] = Form(None),
    tradeset: str = Form(...),
    setkey: str = Form(...),
    svrno: str = Form(...),
    limityn: Optional[str] = Form(None),
    limitamt: Optional[str] = Form(None),
        db: AsyncSession = Depends(get_db),
):
    sno = int(setno)
    uno = int(userno)
    slot = int(slot)
    coink = coinn
    price = initprice.replace(',', '') if initprice else ''
    askrate = lcrate
    lcchk_val = lcchk
    tradeset_split = tradeset.split(',')
    tradeset_val = tradeset_split[0]
    bidsetps = tradeset_split[1] if len(tradeset_split) > 1 else None
    hno = tradeset_split[1] if len(tradeset_split) > 1 else None
    dyn = 'Y' if limityn == 'on' else 'N'
    limityn_value = 'Y' if limityn == 'on' else 'N'
    lmtamt = (limitamt or '').replace(',', '') if limitamt else ''
    bidrate = 1.0 if lcchk_val == 'on' else 0.0
    svrno = int(svrno)
    await editbidsetup(sno, uno, setkey, price, bidsetps, bidrate, askrate, coink, svrno,
                tradeset_val, hno, dyn, lmtamt, limityn_value, slot, db
            )
    return RedirectResponse(url=f"/mytradestat/{uno}/{setkey}/{slot}", status_code=303)

@app.post("/changemypass")
async def change_password(
    data: dict = Body(...),  # JSON body를 dict로 받음
    db: AsyncSession = Depends(get_db)
):
    sql = text("UPDATE traceUser SET userPasswd = PASSWORD(:passwd) WHERE userNo = :userno")
    await db.execute(sql, {"passwd": data["passwd"], "userno": data["uno"]})
    await db.commit()
    return {"result": "success"}

@app.post("/updateuserdtl")
async def update_userdetail(request:Request,
    uno: str = Form(...),
    apikey1: str = Form(...),
    apikey2: str = Form(...),
    svrno: str = Form(...),
    db: AsyncSession = Depends(get_db),
):
    setkey = request.session.get("setKey")
    await update_userdtl(uno, apikey1, apikey2, svrno, db)
    return RedirectResponse(url=f"/userEdit/{uno}/{setkey}", status_code=303)

@app.get('/rest_getorder/{userno}/{setkey}/{slot}')
async def restgetorder(request:Request ,userno:int,setkey:str,slot:int,db: AsyncSession = Depends(get_db)):
    try:
        orderlist = await get_orderlist(userno, setkey, slot, db)
        return JSONResponse({"success": True, "data": orderlist})
    except Exception as e:
        print("Error!!", e)
        return JSONResponse({"success": False, "data": [] })

@app.get('/rest_getmtorder/{userno}/{setkey}')
async def restgetmtorder(request:Request ,userno:int,setkey:str,db: AsyncSession = Depends(get_db)):
    try:
        orderlist = await get_mtorderlist(userno, setkey,db)
        return JSONResponse({"success": True, "data": orderlist})
    except Exception as e:
        print("Error!!", e)
        return JSONResponse({"success": False, "data": [] })

@app.post('/cancelOrder')
async def cancelorder(request:Request,uno: int = Form(...), setkey: str = Form(...), uuid: str = Form(...),db: AsyncSession = Depends(get_db)):
    try:
        keys = await getKeys(uno,setkey,db)
        upbit = pyupbit.Upbit(keys[0],keys[1])
        order = upbit.cancel_order(uuid)
        return JSONResponse({"success": True, "data": order})
    except Exception as e:
        print("주문취소 에러",e)

@app.post('/setyns')
async def setyns(request:Request,setno: int = Form(...), yn: str = Form(...),db: AsyncSession = Depends(get_db)):
    try:
        await setonoffs(setno, yn, db)
        return JSONResponse({"success": True, "data": yn})
    except Exception as e:
        return JSONResponse({"success": False, "data": yn})

@app.post('/setautostop')
async def setatstop(request:Request,sno: int = Form(...), yesno: str = Form(...),db: AsyncSession = Depends(get_db)):
    try:
        await setautostop(sno, yesno, db)
        return JSONResponse({"success": True, "data": yesno})
    except Exception as e:
        return JSONResponse({"success": False, "data": yesno})

@app.post('/setlosscut')
async def setlosscut(request:Request,sno: int = Form(...), rate: float = Form(...), onoff: float = Form(...), db: AsyncSession = Depends(get_db)):
    try:
        await setlconoff(sno, rate, onoff, db)
        return JSONResponse({"success": True, "data": rate})
    except Exception as e:
        return JSONResponse({"success": False, "data": rate})

@app.websocket("/ws/coinprice")
async def coin_price_ws(websocket: WebSocket):
    await websocket.accept()
    coins = websocket.query_params.get("coins", "")
    coin_list = coins.split(",") if coins else []
    try:
        async for market, current_price, change in upbit_ws_price_stream(coin_list):
            await websocket.send_json({"market": market, "current_price": current_price, "change": change})
    except WebSocketDisconnect:
        print(f"WebSocket disconnected: coins {coin_list}")
    except Exception as e:
        print("WebSocket Error:", e)

async def upbit_ws_price_stream(markets: list):
    uri = "wss://api.upbit.com/websocket/v1"
    subscribe_data = [{
        "ticket": "test",
    }, {
        "type": "ticker",
        "codes": markets,
        "isOnlyRealtime": True
    }]
    async with websockets.connect(uri, ping_interval=60) as websocket:
        await websocket.send(json.dumps(subscribe_data))
        while True:
            data = await websocket.recv()
            parsed = json.loads(data)
            yield parsed['code'], parsed['trade_price'], parsed['change']

async def upbit_ws_orderbook_stream(markets: list):
    import websockets
    import json
    uri = "wss://api.upbit.com/websocket/v1"
    subscribe_data = [{
        "ticket": "test",
    }, {
        "type": "orderbook",
        "codes": markets,
        "isOnlyRealtime": True
    }]
    async with websockets.connect(uri, ping_interval=60) as websocket:
        await websocket.send(json.dumps(subscribe_data))
        while True:
            data = await websocket.recv()
            parsed = json.loads(data)
            # orderbook 타입만 처리
            if parsed.get("type") == "orderbook":
                market = parsed.get("code")
                orderbook_units = parsed.get("orderbook_units")
                if market and orderbook_units:
                    yield {
                        "market": market,
                        "orderbook_units": orderbook_units
                    }

@app.websocket("/ws/orderbook")
async def coin_orderbook_ws(websocket: WebSocket):
    await websocket.accept()
    coins = websocket.query_params.get("coins", "")
    coin_list = coins.split(",") if coins else []
    try:
        async for ob_data in upbit_ws_orderbook_stream(coin_list):
            await websocket.send_json(ob_data)
    except WebSocketDisconnect:
        print(f"WebSocket disconnected: coins {coin_list}")
    except Exception as e:
        print("WebSocket Error:", e)

async def upbit_ws_trade_stream(markets: list):
    uri = "wss://api.upbit.com/websocket/v1"
    subscribe_data = [{
        "ticket": "test",
    }, {
        "type": "trade",
        "codes": markets,
        "isOnlyRealtime": True
    }]
    async with websockets.connect(uri, ping_interval=60) as websocket:
        await websocket.send(json.dumps(subscribe_data))
        while True:
            data = await websocket.recv()
            parsed = json.loads(data)
            # trade 타입만 처리
            if parsed.get("type") == "trade":
                market = parsed.get("code")
                trade_price = parsed.get("trade_price")
                trade_volume = parsed.get("trade_volume")
                ask_bid = parsed.get("ask_bid")  #"BID"(매수),"ASK"(매도)
                trade_time = parsed.get("trade_time")
                trade_timestamp = parsed.get("trade_timestamp")
                if market and trade_price and trade_volume:
                    yield {
                        "market": market,
                        "trade_price": trade_price,
                        "trade_volume": trade_volume,
                        "ask_bid": ask_bid,
                        "trade_time": trade_time,
                        "trade_timestamp": trade_timestamp
                    }

@app.websocket("/ws/trade")
async def coin_trade_ws(websocket: WebSocket):
    await websocket.accept()
    coins = websocket.query_params.get("coins", "")
    coin_list = coins.split(",") if coins else []
    try:
        async for trade_data in upbit_ws_trade_stream(coin_list):
            await websocket.send_json(trade_data)
    except WebSocketDisconnect:
        print(f"WebSocket disconnected: coins {coin_list}")
    except Exception as e:
        print("WebSocket Error:", e)


@app.get('/upbittop30/{uno}/{setkey}')
async def upbittop30(request:Request,uno:int,setkey:str,db: AsyncSession = Depends(get_db)):
    userName = request.session.get("user_Name")
    userRole = request.session.get("user_Role")
    setkey = request.session.get("setKey")
    trcnt = request.session.get("License")
    serverno = request.session.get("server_No")
    coins = pyupbit.get_tickers(fiat="KRW")
    return templates.TemplateResponse('/trade/upbittop30.html', {"request": request, "trcnt": trcnt, "user_Name": userName, "setkey": setkey, "user_No": uno, "user_Role": userRole, "coins":coins ,
                                       "server_No": serverno})

@app.get('/api/mtpondsetup/{userno}')
async def mtpondsetup_all(userno: int, db: AsyncSession = Depends(get_db)):
    sql = text("SELECT activeYN,initAmt,addAmt,limitAmt,minMargin,maxMargin,tickRate,tickYN,lcRate,lcGap,maxCoincnt, martinYN, stopYN, stopAutoYN FROM mtSetup WHERE userNo = :userno AND attrib NOT LIKE :attrib")
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
async def myorders(userno:int,setkey:str , db: AsyncSession = Depends(get_db)):
    try:
        myorders = await api_mtorderlist(userno,db)
        cprices = await get_current_prices()
        return JSONResponse({"success": True, "data": myorders, "cprices": cprices})
    except Exception as e:
        return JSONResponse({"success": False, "data": [], "cprices": []})

@app.get("/phapp/mlogin/{userid}/{passwd}")
async def mlogin(userid: str, passwd: str, db: AsyncSession = Depends(get_db)):
    result = None
    try:
        query = text("SELECT userNo, userName,setupKey from traceUser where userId = :userid and userPasswd = PASSWORD(:passwd)")
        r = await db.execute(query, {"userid": userid, "passwd": passwd})
        rows = r.fetchone()
        if rows is None:
            return {"error": "No data found for the given data."}
        result = {"userno": rows[0], "username": rows[1], "setupkey": rows[2]}
    except:
        print("mLogin error")
    finally:
        return result


@app.get("/api/balance/{userno}/{setkey}")
async def api_my_balance(
        request: Request,
        userno: int,setkey: str,
        db: AsyncSession = Depends(get_db)
):
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
async def excoin(request:Request, userNo:int, setkey:str, db: AsyncSession = Depends(get_db)):
    excoinlist = None
    coinlist = pyupbit.get_tickers(fiat="KRW")
    try:
        query = text("SELECT DISTINCT market FROM exCoinlist WHERE userNo in (0, :userno)  and attrib NOT LIKE :attrib")
        r = await db.execute(query, {"userno": userNo, "attrib": "%XXX%"})
        rows = r.fetchall()
        if rows is None:
            return {"error": "No data found for the given data."}
        excoinlist = [r[0] for r in rows]
    except:
        print("excoinlist error")
    finally:
        return templates.TemplateResponse('/trade/excoin.html', {"request": request, "user_No": userNo,"setkey":setkey, "coinlist":coinlist, "excoinlist":excoinlist})


@app.post("/setexCoin/{userNo}")
async def setexcoin(request:Request, userNo: int, selcoin: Optional[List[str]] = Form(default=None, alias="selcoin[]"), db: AsyncSession = Depends(get_db)):
    selcoin = selcoin or []
    try:
        query  = text("UPDATE exCoinlist set attrib = :attx WHERE userNo = :userno")
        await db.execute(query, {"attx":"XXXUPXXXUP", "userno": userNo})
        for coin in selcoin:
            query = text("INSERT INTO exCoinlist (userNo, market) values (:userNo, :market)")
            await db.execute(query, {"userNo": userNo, "market": coin})
        await db.commit()
        return RedirectResponse(url=f"/excoinlist/{userNo}/{request.session.get('setKey')}", status_code=303)
    except:
        print("setexcoin error")
