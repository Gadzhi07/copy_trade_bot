import datetime
import re

import os
import sys

import asyncio
import aiohttp
from binance.client import AsyncClient
import binance

from sql import *
from main import send_m

from loguru import logger


logger.add("logs/log_{time}.log", rotation="55 MB", format="{time} | {level}: {message}")


# ------------------ Нужно переписать на async ------------------
def num_decimal_places(value):
    # находит количество знаков после запятой
    m = re.match(r"^[0-9]*\.([1-9]([0-9]*[1-9])?)0*$", value)
    return len(m.group(1)) if m is not None else 0


async def get_open_pos(symbol: str, pos_side: str, client=None, api_key: str = None, secret_key: str = None):
    try:
        if api_key is not None and secret_key is not None:
            client = await AsyncClient.create(api_key, secret_key)
        positions = await client.futures_position_information(symbol=symbol)
        for position in positions:
            if position['positionSide'] == pos_side:
                return position
    except binance.client.BinanceAPIException as e:
        if e.code == -2015:
            return "Неправильно введён публичный API ключ или секретный API ключ"
        else:
            if api_key is not None:
                logger.error(str(e) + " | " + str(api_key))
            if client is not None:
                logger.error(str(e) + " | " + str(tuple(client)))
            return "Неизвестная ошибка\n" + str(e.message)


def select_pos(order, user, trader):
    # выберает позицию
    if order['positionSide'] != 'BOTH':
        pos = select_4('positions', 'symbol', order['symbol'], 'position_side', order['positionSide'], 'user_id'
                       , str(user), 'trader', str(trader))
    elif order['positionSide'] == 'BOTH':
        pos = select_3('positions', 'symbol', order['symbol'], 'user_id', str(user), 'trader', str(trader))
    return pos


def insert_pos(order, user, trader):
    # открывает позицию
    pos = select_pos(order, user, trader)
    if pos is None:
        data = [(order['symbol'], order['positionSide'], str(user), str(trader))]
        try:
            insert('positions', data)
            logger.info(str(user) + ' | insert positions')
        except Exception as e:
            logger.error(str(user) + " | " + str(e) + " | insert_pos")


def del_pos(user, symbol, position_side):
    # удаляет позицию
    try:
        delete_3('positions', 'symbol', symbol, 'position_side', position_side,
                 'user_id', str(user))
        logger.info(str(user) + ' | delete')
    except Exception as e:
        logger.error(str(user) + " | " + str(e) + " | del_pos")


# ------------------ Отмена ордеров ------------------
async def cancel_orders(trader, id_options, trader_id):
    orders = await trader.futures_get_all_orders(limit=11)
    await trader.close_connection()
    orders = orders[::-1]
    users = find('users')
    # для каждого пользователя , у которого включён бот отменять ордера
    for user in users:
        if user[10] == "on" and str(user[1]) in id_options and str(user[4]) == '1':
            client = None
            try:
                client = await AsyncClient.create(user[5], user[6])
                open_ords = await client.futures_get_open_orders()
                if open_ords is not None and open_ords != [] and orders is not None and orders != []:
                    for open_ord in open_ords:
                        for order in orders:
                            now = datetime.datetime.now()
                            delta = now - datetime.timedelta(minutes=2)
                            date = datetime.datetime.fromtimestamp(order['time'] / 1e3)
                            if delta < date:
                                # иногда avgPrice и price бывают нулями
                                if open_ord['type'] == 'TRAILING_STOP_MARKET':
                                    context = 'priceRate'
                                elif float(open_ord['avgPrice']) > 0.0:
                                    context = 'avgPrice'
                                elif float(open_ord['price']) > 0.0:
                                    context = 'price'
                                else:
                                    context = 'stopPrice'
                                pos = select_pos(order, user[0], trader_id)
                                if (pos is not None and str(pos[3]) == str(trader_id)) or pos is None:
                                    # если ордер отменён или истёк,то отменяем ордер, оповещаем в боте и удаляем позицию
                                    if order['symbol'] == open_ord['symbol'] and (
                                            order['status'] == "CANCELED" or order['status'] == "EXPIRED") and \
                                            float(order[context]) == float(open_ord[context]):
                                        logger.info(str(user[5]) + ' | ' + str(user[6]))
                                        logger.info('close')
                                        try:
                                            await send_m(user[0], order, open_ord['origQty'], "CANCEL")
                                            await client.futures_cancel_order(symbol=order['symbol'],
                                                                              orderId=open_ord['orderId'])
                                        except binance.client.BinanceAPIException as e:
                                            logger.error(e)

                all_pos_and_ords = []
                if open_ords is not None and open_ords != []:
                    for open_ord in open_ords:
                        if [open_ord['symbol'], open_ord['positionSide']] not in all_pos_and_ords:
                            all_pos_and_ords.append([open_ord['symbol'], open_ord['positionSide']])

                positions = await client.futures_account()
                if positions['positions'] is not None and positions['positions'] != []:
                    for position in positions['positions']:
                        if abs(float(position['positionAmt'])) > 0.0 and \
                                [position['symbol'], position['positionSide']] not in all_pos_and_ords:
                            all_pos_and_ords.append([position['symbol'], position['positionSide']])

                positions_in_base = select_all('positions', 'user_id', user[0])
                if positions_in_base is not None and positions_in_base != []:
                    for position_in_base in positions_in_base:
                        if position_in_base is not None and \
                                [position_in_base[0], position_in_base[1]] not in all_pos_and_ords:
                            logger.info(positions_in_base)
                            logger.info(all_pos_and_ords)
                            del_pos(user[0], position_in_base[0], position_in_base[1])

                await client.close_connection()
            except Exception as e:
                if client is not None:
                    await client.close_connection()
                logger.error(str(user[0]) + str(e) + " | " + str(trader))


# ------------------ Открытие ордеров ------------------
async def test_ord1():
    while True:
        # берём всех трейдеров
        traders = find('traders')
        for info_trader in traders:
            # ищем все опции , в которых есть этот трейдер
            options = select_all("options", 'traders_id', info_trader[0])
            id_options = []
            for option in options:
                id_options.append(option[0])
            # открываем сессию и ищем ордера , которые были в ближайшую минуту
            trader = await AsyncClient.create(info_trader[2], info_trader[3])
            pos_mode = await trader.futures_get_position_mode()
            pos_mode = pos_mode['dualSidePosition']
            info = await trader.futures_get_all_orders(limit=6)
            await cancel_orders(trader, id_options, info_trader[0])
            await trader.close_connection()
            for order in info:
                now = datetime.datetime.now()
                delta = now - datetime.timedelta(minutes=2)
                date = datetime.datetime.fromtimestamp(order['time'] / 1e3)
                if delta < date:
                    # если ордера нету в базе ордеров , то записываем его и открываем ордер
                    f_base = select("orders", "id", order['orderId'])
                    if f_base:
                        pass
                    else:
                        if order['type'] == 'TRAILING_STOP_MARKET':
                            data = [(order['orderId'], order['symbol'], order['type'], order['activatePrice'])]
                        elif float(order['avgPrice']) > 0.0:
                            data = [(order['orderId'], order['symbol'], order['type'], order['avgPrice'])]
                        elif float(order['price']) > 0.0:
                            data = [(order['orderId'], order['symbol'], order['type'], order['price'])]
                        else:
                            data = [(order['orderId'], order['symbol'], order['type'], order['stopPrice'])]
                        insert("orders", data)
                        order_info = await get_open_pos(symbol=order['symbol'], pos_side=order['positionSide'],
                                                        api_key=info_trader[2], secret_key=info_trader[3])
                        # открытие ордеров
                        await create_orders(order, order_info, pos_mode, id_options, info_trader[0])


async def create_orders(order, order_info, pos_mode, id_options, trader_id):
    # проходим по пользователям и вызываем функцию для открытия ордеров
    users = find('users')
    for user in users:
        if user[10] == "on" and str(user[1]) in id_options and str(user[4]) == '1':
            try:
                # если ордер не отменён и не истёк
                if order['status'] != "CANCELED" and order['status'] != 'EXPIRED':
                    client = await AsyncClient.create(user[5], user[6])
                    bal = await client.futures_account()
                    bal = float(bal['availableBalance'])
                    qty_pos = float(float(bal) * (float(user[11]) * 0.01))
                    qty_usdt = user[12]
                    # открываем ордера
                    if order['type'] == 'TRAILING_STOP_MARKET':
                        var = "activatePrice"
                    elif float(order['avgPrice']) > 0.0:
                        var = "avgPrice"
                    elif float(order['price']) > 0.0:
                        var = "price"
                    else:
                        var = 'stopPrice'
                    try:
                        await new_ord(client, user[0], order, order_info, qty_pos, qty_usdt, pos_mode, trader_id, var)
                    except binance.client.BinanceAPIException as e:
                        logger.error(str(e))
            except Exception as e:
                logger.error(e)


async def new_ord(client, user, order, trader_info, qty, qty_usdt, pos_mode, trader_id, context):
    # меняем мод на хедж или односторонний
    try:
        await client.futures_change_position_mode(dualSidePosition=pos_mode)
    except binance.client.BinanceAPIException as e:
        if e.code == -4059:
            pass
        else:
            logger.error(str(e.code) + " : " + str(e.message))
    client_info = await get_open_pos(symbol=order['symbol'], pos_side=order['positionSide'], client=client)
    pos = select_pos(order, user, trader_id)
    logger.info(client_info)
    logger.info(trader_info)
    logger.info(order)
    logger.info(pos)
    if float(client_info['positionAmt']) > 0.0 and not pos:
        pass
    elif (pos is not None and str(pos[3]) == str(trader_id)) or pos is None:
        shoulder = float(trader_info['leverage'])
        margin_type = trader_info["marginType"]
        # если у трейдера размер позиции больше 0
        if client_info['positionSide'] == order['positionSide'] and abs(float(trader_info['positionAmt'])) > 0.0:
            # --------- Зайти в позицию ---------
            # if float(client_info['positionAmt']) == 0.0 and (pos is None or (order['status'] == 'FILLED'
            #                                                                  and abs(
            #             float(trader_info['positionAmt'])) == abs(float(order['origQty'])))):
            if float(client_info['positionAmt']) == 0.0 and pos is None:
                # Если у трейдер есть открытая позиция, то сюда не попадает
                if (order['status'] != 'FILLED' and order['type'] != 'MARKET' and
                    abs(float(trader_info['positionAmt'])) == 0.0) or \
                        ((order['status'] == 'FILLED' or order['type'] == 'MARKET') and
                         abs(float(trader_info['positionAmt'])) - abs(float(order['origQty'])) == 0):
                    count = round((qty / float(order[context])) * shoulder,
                                  num_decimal_places(str(order['origQty'])))
                    send_m_text = "NEW"
                    try:
                        await new_orders(client, order, margin_type, shoulder, count, pos_mode)
                        insert_pos(order, user, trader_id)
                        await send_m(user, order, count, send_m_text)
                    except binance.client.BinanceAPIException:
                        pass
            # --------- Усреднить или Закрыть ---------
            else:
                find_percent = 0
                # --------- Усреднить ЛОНГ/ШОРТ ---------
                if (client_info['positionSide'] == order['positionSide'] == "LONG" and order['side'] == "BUY") or \
                        (client_info['positionSide'] == order['positionSide'] == "SHORT" and
                         order['side'] == "SELL") or (client_info['positionSide'] == order['positionSide'] == 'BOTH'
                                                      and order['side'] == pos[1]):
                    count = float(float(abs(float(client_info['positionAmt']))) * (float(qty_usdt) * 0.01))
                    count = round(count, num_decimal_places(str(client_info['positionAmt'])))
                    # if float(count) * float(order[context]) <= 5 and not (order['status'] == 'FILLED'
                    #                                                       and abs(
                    #             float(trader_info['positionAmt'])) == abs(float(order['origQty']))):
                    if float(count) * float(order[context]) <= 5:
                        count = 0.0
                # --------- Закрыть ЛОНГ/ШОРТ ---------
                elif (client_info['positionSide'] == order['positionSide'] == "LONG" and order['side'] == "SELL") \
                        or (
                        client_info['positionSide'] == order['positionSide'] == "SHORT" and order['side'] == "BUY") \
                        or (
                        client_info['positionSide'] == order['positionSide'] == 'BOTH' and order['side'] != pos[1]):
                    if order['type'] != 'MARKET' and abs(float(order['origQty'])) == \
                            abs(float(trader_info['positionAmt'])) and order['status'] != 'FILLED':
                        find_percent = 100.0
                    else:
                        if order['type'] == 'MARKET':
                            find_percent = round(float(order['origQty']) /
                                                 (abs(float(trader_info['positionAmt'])) + float(order['origQty']))
                                                 * 100.0, 0)
                        else:
                            find_percent = round(float(order['origQty']) / abs(float(trader_info['positionAmt']))
                                                 * 100.0, 0)
                    # position_info = get_open_pos(order['symbol'], order['positionSide'], client=client)
                    if abs(float(client_info['positionAmt'])) > 0:
                        count = float(float(abs(float(client_info['positionAmt']))) * (float(find_percent) * 0.01))
                    if str(count)[-1] == '5' or len(str(count)) > len(str(order['origQty'])):
                        nulls_ = '{:0' + str(num_decimal_places(str(count))) + '}'
                        round_number_ = float(nulls_.format(1)[0] + '.' + nulls_.format(1)[1:])
                        count = -1 * count // round_number_ * -round_number_
                    if order['closePosition'] is True:
                        count = abs(float(client_info['positionAmt']))
                    send_m_text = f"CLOSE {order['positionSide']}"
                if find_percent != 100.0 and order['reduceOnly'] is False:
                    nulls_ = '{:0' + str(num_decimal_places(str(count))) + '}'
                    round_number_ = float(nulls_.format(1)[0] + '.' + nulls_.format(1)[1:])
                    count = -1 * count // round_number_ * -round_number_
                    send_m_text = f"AVERAGE {order['positionSide']}"
                elif find_percent != 100.0 and order['reduceOnly'] is True:
                    count = round(count, num_decimal_places(str(client_info['positionAmt'])))
                try:
                    await new_orders(client, order, margin_type, shoulder, count, pos_mode)
                    await send_m(user, order, count, send_m_text)
                except binance.client.BinanceAPIException:
                    pass

        # если новый ордер (позиция трейдера равна нулю)
        elif client_info['positionSide'] == order['positionSide'] and order['status'] == 'NEW' and abs(
                float(client_info['positionAmt'])) == 0.0:
            count = round((qty / float(order[context])) * shoulder, num_decimal_places(str(order['origQty'])))
            try:
                await new_orders(client, order, margin_type, shoulder, count, pos_mode)
                await send_m(user, order, count, f" {order['positionSide']}")
                insert_pos(order, user, trader_id)
            except binance.client.BinanceAPIException:
                pass
        # если полностью закрывается ордер
        elif client_info['positionSide'] == order['positionSide']:
            count = abs(float(client_info['positionAmt']))
            try:
                await new_orders(client, order, margin_type, shoulder, count, pos_mode)
                await send_m(user, order, count, f"CLOSE {client_info['positionSide']}")
            except binance.client.BinanceAPIException:
                pass
        logger.info(str(count) + " | " + str(user) + " | new_ord")
        # await client.close_connection()


async def new_orders(client, order, margin_type, shoulder, count, pos_mode):
    # процесс открытия / закрытия ордеров и позиций
    if margin_type == "cross":
        margin_type = "CROSSED"
    if margin_type == "isolated":
        margin_type = "ISOLATED"
    try:
        await client.futures_change_leverage(symbol=order['symbol'], leverage=int(shoulder))
        await client.futures_change_margin_type(symbol=order['symbol'], marginType=margin_type)
    except binance.client.BinanceAPIException as e:
        if e.code == -4028:
            await client.futures_change_margin_type(symbol=order['symbol'], marginType=margin_type)
        elif e.code != -4046:
            logger.error(str(count) + str(client) + str(order))
    params = {'symbol': order['symbol'], 'type': order['type'], 'positionSide': order['positionSide'],
              'side': order['side'], 'quantity': abs(count)}
    if order['type'] == "LIMIT":
        params['price'] = float(order['price'])
        params['timeInForce'] = order['timeInForce']
    if order['type'] == "MARKET":
        pass
    if order['type'] == 'STOP' or order['type'] == "TAKE_PROFIT":
        params['price'] = float(order['price'])
        params['stopPrice'] = float(order['stopPrice'])
        params['priceProtect'] = order['priceProtect']
    if order['type'] == "STOP_MARKET" or order['type'] == "TAKE_PROFIT_MARKET":
        params['stopPrice'] = float(order['stopPrice'])
        params['closePosition'] = order['closePosition']
        params['priceProtect'] = order['priceProtect']
        if params['closePosition'] is True:
            params.pop('quantity')
    elif order['type'] == 'TRAILING_STOP_MARKET':
        params['callbackRate'] = float(order['priceRate'])
        params['workingType'] = order['workingType']
        params['activationPrice'] = float(order['activatePrice'])

    if pos_mode is False and order['closePosition'] is False:
        params['reduceOnly'] = order['reduceOnly']
    logger.info(params)
    if params['quantity'] != 0.0:
        try:
            await client.futures_create_order(**params)
        except binance.client.BinanceAPIException as e:
            if e.code == -2021:
                params.pop('activationPrice')
                await client.futures_create_order(**params)
            else:
                logger.error(str(e) + " | new_orders")
    if params['quantity'] == 0.0:
        logger.debug("count - " + str(params['quantity']) + " | new_orders")
    await client.close_connection()


if __name__ == '__main__':
    try:
        logger.info('Start')
        asyncio.get_event_loop().run_until_complete(test_ord1())
        os.execv(__file__, sys.argv)
    except KeyboardInterrupt:
        logger.info('End')
    except aiohttp.ClientConnectorError:
        logger.debug('ConnectionError: Please turn on Wi-Fi')
        os.execv(__file__, sys.argv)
