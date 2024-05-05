import db_coin
import logging
from flask import Flask
from flaskext.mysql import MySQL
from pymysql.cursors import DictCursor
import decimal
import commons
import constants

logging.getLogger('__main__')

#TODO: DB config 정리
app = Flask(__name__)
app.config['MYSQL_DATABASE_USER'] = 'root'
app.config['MYSQL_DATABASE_PASSWORD'] = 'root'
app.config['MYSQL_DATABASE_DB'] = 'coin'
app.config['MYSQL_DATABASE_HOST'] = '127.0.0.1'
mysql = MySQL(cursorclass=DictCursor)
mysql.init_app(app)
conn = mysql.connect()

def obtain_near_tier_by_price(side, price, coin):
    if side == constants.SIDE.BUY:
        lookup = obtain_lookup (coin, constants.ORDERBY.DESC)
        for each_tier in lookup:
            if price > each_tier["buy_adjust_price"]:
                return obtain_tier_by_lookup_index(each_tier["index"] + 1, coin)
            elif price == each_tier["buy_adjust_price"]:
                return each_tier

    if side == constants.SIDE.SELL:
        lookup = obtain_lookup (coin, constants.ORDERBY.ASC)
        for each_tier in lookup:
            if price < each_tier["sell_adjust_price"]:
                return obtain_tier_by_lookup_index(each_tier["index"] - 1, coin)
            elif price == each_tier["sell_adjust_price"]:
                return each_tier
    
    if coin["auto_increment_number_of_tiers"] == True:
        coin["number_of_tiers"] = coin["number_of_tiers"] * 2
        db_coin.update_number_of_tiers(coin["market"], coin["number_of_tiers"])
        lookup = generate(coin)
        return obtain_near_tier_by_price(side, price, coin)
    else:
        logging.error("exceed allocated tiers. market: %s, price: %s", coin["market"], price)
        return None

def obtain_tier_by_price(side, price, coin):

    if side == constants.SIDE.BUY:
        lookup = obtain_lookup (coin, constants.ORDERBY.DESC)
        for each_tier in lookup:
            if price == each_tier["buy_adjust_price"]:
                return each_tier

    if side == constants.SIDE.SELL:
        lookup = obtain_lookup (coin, constants.ORDERBY.ASC)
        for each_tier in lookup:
            if price == each_tier["sell_adjust_price"]:
                return each_tier
    
    if coin["auto_increment_number_of_tiers"] == True:
        coin["number_of_tiers"] = coin["number_of_tiers"] * 2
        db_coin.update_number_of_tiers(coin["market"], coin["number_of_tiers"])
        lookup = generate(coin)
        return obtain_tier_by_price(side, price, coin)
    else:
        logging.error("exceed allocated tiers. market: %s, price: %s", coin["market"], price)
        return None

def obtain_tier_by_lookup_index(index, coin):

    lookup = obtain_lookup (coin, constants.ORDERBY.ASC)

    for tier in lookup:
        if tier["index"] == index:
            return tier
    
    if tier == None:
        if coin["auto_increment_number_of_tiers"] == True:
            coin["number_of_tiers"] = coin["number_of_tiers"] * 2
            db_coin.update_number_of_tiers(coin["market"], coin["number_of_tiers"])
            lookup = generate(coin)
            return obtain_tier_by_lookup_index(index, coin)
        else:
            logging.error("exceed allocated tiers. market: %s, index: %s", coin["market"], index)
            return None

    return None

def obtain_lookup(coin, orderby):

    if constants.ORDERBY.ASC == orderby:
        lookup = select_order_by_asc(coin)
    else: 
        lookup = select_order_by_desc(coin)

    if lookup == None or len(lookup) == 0:
        generate(coin)
        return obtain_lookup(coin, orderby)
    else:
        return lookup

def generate(coin):

    # 해당 코인 lookup 전체 삭제
    delete(coin)

    tier_list = []

    # initial price 기준으로 위로 tiers 수만큼 생성
    for x in range(coin["number_of_tiers"]):
        buy_price = coin["initial_price"] + (coin["initial_price"] * ((coin["buy_percentage"] / decimal.Decimal(100)) * decimal.Decimal(x)))
        buy_adjust_price = commons.adjust_price_by_price_unit(buy_price)
        buy_adjust_price_percentage = round(decimal.Decimal(100) + (coin["buy_percentage"] * x),4)
        sell_price = buy_price + (buy_price * (coin["sell_percentage"] / decimal.Decimal(100)))
        sell_adjust_price = commons.adjust_price_by_price_unit(sell_price)
        tier_list.append((x, buy_price, buy_adjust_price, buy_adjust_price_percentage, coin["market"], sell_price, sell_adjust_price))

    # initial price 기준으로 아래로 tier 수만큼 생성
    for x in range(coin["number_of_tiers"]):
        if x == 0:
            continue
        buy_price = coin["initial_price"] - (coin["initial_price"] * ((coin["buy_percentage"] / decimal.Decimal(100)) * decimal.Decimal(x)))
        sell_price = buy_price + (buy_price * (coin["sell_percentage"] / decimal.Decimal(100)))

        if buy_price <= decimal.Decimal(0) or sell_price <= decimal.Decimal(0):
            continue

        buy_adjust_price = commons.adjust_price_by_price_unit(buy_price)
        buy_adjust_price_percentage = round(decimal.Decimal(100) - (coin["buy_percentage"] * x),4)
        sell_adjust_price = commons.adjust_price_by_price_unit(sell_price)
        tier_list.append((x * (-1), buy_price, buy_adjust_price, buy_adjust_price_percentage, coin["market"], sell_price, sell_adjust_price))

    insert(tier_list)

    lookup = lookup_tuple_to_dict(tier_list)

    return lookup

def lookup_tuple_to_dict(tier_list):
    lookup_list = []
    for tier in tier_list:
        lookup_dict = {}
        lookup_dict["index"] = tier[0]
        lookup_dict["buy_price"] = tier[1]
        lookup_dict["buy_adjust_price"] = tier[2]
        lookup_dict["buy_adjust_price_percentage"] = tier[3]
        lookup_dict["market"] = tier[4]
        lookup_dict["sell_price"] = tier[5]
        lookup_dict["sell_adjust_price"] = tier[6]
        lookup_list.append(lookup_dict)
    return lookup_list

def delete(coin):
    # 해당 코인 lookup 전체 삭제
    conn = mysql.connect()

    try:
        cursor = conn.cursor()
        query = "DELETE FROM `coin`.`tier_lookup` WHERE `market` = %s"
        data = (coin["market"])
        cursor.execute(query, data)
        conn.commit()
    except Exception as e:
        logging.error("exception occurs.", e)
        return None
    finally:
        conn.close()

def insert(tier_list):
    conn = mysql.connect()

    try:
        cursor = conn.cursor()
        for tier in tier_list:
            query = "INSERT INTO `coin`.`tier_lookup` (`index`,`buy_price`,`buy_adjust_price`,`buy_adjust_price_percentage`,`market`,`sell_price`,`sell_adjust_price`) VALUES (%s, %s, %s, %s, %s, %s, %s);"
            cursor.execute(query, tier)
        conn.commit()
    except Exception as e:
        logging.error("exception occurs.", e)
        return None
    finally:
        conn.close()

def select_order_by_asc (coin):
    conn = mysql.connect()
    try:
        cursor = conn.cursor()
        query = "SELECT `tier_lookup`.`index`,`tier_lookup`.`buy_price`,`tier_lookup`.`buy_adjust_price`,`tier_lookup`.`buy_adjust_price_percentage`,`tier_lookup`.`market`,`tier_lookup`.`sell_price`,`tier_lookup`.`sell_adjust_price` FROM `coin`.`tier_lookup` WHERE `tier_lookup`.`market`= %s order by `tier_lookup`.`index` asc"
        data = (coin["market"])
        cursor.execute(query, data)
        results = cursor.fetchall()
        return results
    except Exception as e:
        logging.error("exception occurs.", e)
        return None
    finally:
        conn.close()

def select_order_by_desc (coin):
    conn = mysql.connect()
    try:
        cursor = conn.cursor()
        query = "SELECT `tier_lookup`.`index`,`tier_lookup`.`buy_price`,`tier_lookup`.`buy_adjust_price`,`tier_lookup`.`buy_adjust_price_percentage`,`tier_lookup`.`market`,`tier_lookup`.`sell_price`,`tier_lookup`.`sell_adjust_price` FROM `coin`.`tier_lookup` WHERE `tier_lookup`.`market`= %s order by `tier_lookup`.`index` desc"
        data = (coin["market"])
        cursor.execute(query, data)
        results = cursor.fetchall()
        return results
    except Exception as e:
        logging.error("exception occurs.", e)
        return None
    finally:
        conn.close()