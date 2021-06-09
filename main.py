import ccxt, sqlite3, argparse, time, os
import pandas as pd


def download_binance_futures_data(db_path="/home/ych/Storage/binance/binance_futures.db", symbols="all"):
    # DB 초기화
    db = sqlite3.connect(db_path)

    # CCXT binance 거래소: 선물을 기본으로 함
    binance = ccxt.binance(
        {
            "options" : {
                "defaultType" : "future"
            },
            "enableRateLimit" : True
        }
    )
    
    # symbols == "all" 이라면 모든 티커 다운로드
    if symbols == "all":
        symbols = [mkt["symbol"] for mkt in binance.fetch_markets()]
    # 그렇지 않다면 입력으로 받은 문자열을 파싱
    else:
        symbols = symbols.split(",")

    # 심볼 개수와 심볼 전체 프린트
    print(f"downloading data for {len(symbols)} symbols : {symbols}")

    for symbol in symbols:
        # 테이블 없다면 DB 만들기
        db.execute(f"""
        CREATE TABLE IF NOT EXISTS _{symbol.replace("/", "")} (
            timestamp int, 
            open float, 
            high float, 
            low float, 
            close float, 
            volume float
        )""")
        # 시간 로깅 용
        t = time.time()

        # 테이블 만들고 커밋
        db.commit()

        prev_data = db.execute(f"SELECT * FROM _{symbol.replace('/', '')}").fetchall()

        # 이전 데이터 존재 여부 확인
        # startTime 옵션을 >=로 비교하여 가져오므로 1을 더해준다
        timestamp = 0 if not prev_data else prev_data[-1][0] + 1 
        downloaded = 0 # 로깅용

        while True:
            # 바이낸스에서 1분봉 받아오기
            tohlcv = binance.fetch_ohlcv(
                symbol=symbol, 
                timeframe="1m", 
                params={"startTime" : timestamp}, 
                limit=1500
            )
            
            # 1분봉이 없다면 -> 타임스탬프가 현재 시점(최신)이므로 루프를 끝냄
            if not tohlcv:
                break
            
            # db에 저장
            for timestamp, open, high, low, close, volume in tohlcv:
                db.execute(f"""
                INSERT INTO _{symbol.replace('/', '')} VALUES (
                    {timestamp}, {open}, {high}, {low}, {close}, {volume}
                )""")
            
            # db에 commit
            db.commit()

            # startTime 옵션을 >=로 비교하여 가져오므로 1을 더해준다
            timestamp = tohlcv[-1][0] + 1

            # 다운로드된 양
            downloaded += len(tohlcv)
            
            # 현재 지난 시간
            delta_t = time.time() - t
            
            # 로깅
            print(f"""downloaded {downloaded} rows for {symbol} in {round(delta_t)} seconds, download speed is {round(downloaded / delta_t)} row per second""", end="\r")
        print(f"""downloaded {downloaded} rows for {symbol} in {round(delta_t)} seconds, download speed is {round(downloaded / delta_t)} row per second""")

        db.commit()


def read_binance_futures_data(db_path, symbol, timeframe):
    # symbol이 BTC/USDT와 같은 형태로 들어오면 DB와 맞지 않으므로 BTCUSDT 처럼 바꿈
    symbol = symbol.replace("/", "")

    # DB에 연결
    db = sqlite3.connect(db_path)

    # fetchall() 메서드 사용해서 DB에서 데이터 받아오기
    data = db.execute(f"SELECT * FROM _{symbol}").fetchall()
    
    # DB에서 받아온 데이터로 pd.DataFrame 만들기
    data = pd.DataFrame(
        data, columns=["timestamp", "open", "high", "low", "close", "volume"])

    # pd.to_datetime은 second보다 더 작은 precision을 지원하므로 1000000을 곱해주어야 정확한 결과가 나오게 됨
    data.index = pd.to_datetime(data["timestamp"] * 1000000)
    
    # timestamp column은 필요가 없으므로 삭제함
    del data["timestamp"]

    # 타임프레임이 기본 (1 minute) 가 아니라면, resample
    if timeframe != "1T":
        data = data.resample(timeframe).agg(
            {
                "open" : "first",
                "high" : "max",
                "low" : "min",
                "close" : "last",
                "volume" : "sum"
            }
        )
        data = data.ffill() # missing data 제거 (binance 서버 터짐 등)
    
    return data


def export_data(db_path, symbols, timeframes, export_dir):
    timeframes = timeframes.split(",")
    
    # 심볼 정하기: 다운로드 코드와 같음
    if symbols == "all":
        binance = ccxt.binance(
            {
                "options" : {
                    "defaultType" : "future"
                },
                "enableRateLimit" : True
            }
        )
        symbols = [mkt["symbol"] for mkt in binance.fetch_markets()]

    else:
        symbols = symbols.split(",")

    # 익스포팅 루프
    for symbol in symbols:
        for timeframe in timeframes:    
            # 데이터 가져오기
            df = read_binance_futures_data(db_path)
            
            # export path: export_dir/symbol_timeframe.csv
            export_path = os.path.join(export_dir, f"{symbol}_{timeframe}.csv")
            
            # csv로 내보내기
            df.to_csv(export_path) 

            print(f"exported data to {export_path}")


if __name__ == "__main__":
    # cli 인터페이스

    # argparse 
    parser = argparse.ArgumentParser()

    # argument #1 db_path: 데이터베이스 경로
    parser.add_argument("--db_path", default="/home/ych/Storage/binance/binance_futures.db", type=str)
    
    # argument #2 symbols: "all"로 설정 시 모든 티커 다운로드, 혹은 ,를 구분자로 하여 스트링으로 입력 가능
    parser.add_argument("--symbols", default='all', type=str)
    
    # 익스포트할 경로, 디렉토리까지만 써 주면 된다. (써져 있으면 익스포트 모드로 동작한다)
    parser.add_argument("--export_dir", default=None, type=str)

    # 익스포트할 timeframe
    parser.add_argument("--export_timeframes", default="1T", type=str)

    # 파싱
    args = parser.parse_args()

    # 다운로드 함수 실행 (다운로드 모드)
    if args.export_dir is None:
        download_binance_futures_data(args.db_path, args.symbols)
    # 익스포트 함수 실행 (익스포트 모드)
    else:
        export_data(args.db_path, args.symbols, args.export_timeframes, args.export_dir)

