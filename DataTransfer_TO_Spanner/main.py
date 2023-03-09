import uuid
import pandas as pd
import configparser
from loguru import logger
from datetime import datetime, timedelta
from db.spanner import SpannerHelper

if __name__ == "__main__":
    logger.add('coupang.log')
    logger.debug('START')
    # config.ini 파일 가져오기
    config = configparser.ConfigParser()
    config.read('config.ini', encoding='utf8')
    tousflux_key = config['xx']['xx'] 
    instance_id  = config['xx']['yy']
    database_id  = config['xx']['zz']
    spanner = SpannerHelper(instance_id, database_id, tousflux_key)

    # csv 파일 읽어오기
    schedules = pd.read_excel('파일명.xlsx', header = 0)

    for schedule in schedules.values:
        try:
            database = spanner.ExecuteQuery(f'''
                                                INSERT INTO 테이블명 (테이블컬럼s)
                                                VALUES('{uuid.uuid4()}',  {schedule[0]},  '{schedule[1]}',  '{schedule[2]}',  '{schedule[3]}',  '{schedule[4]}', '{schedule[5]}', '{schedule[6]}', TIMESTAMP('{schedule[7]}','Asia/Seoul'), TIMESTAMP('{schedule[8]}','Asia/Seoul'), 'N', CURRENT_TIMESTAMP())
                                            ''')
        except Exception as e:
            logger.error(f'[INSERT ERROR]  >>  {e}')