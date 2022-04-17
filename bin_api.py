#!/usr/bin/python3
import re

import asyncio
import aiohttp
import binance
from binance import AsyncClient, BinanceSocketManager

from sql import select, select_4, select_3, select_all, insert, delete_3, find
from main import send_m

from loguru import logger

logger.add("/root/tg_bot/logs/log_{time}.log", rotation="55 MB", format="{time} | {level}: {message}")


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
    if order['ps'] != 'BOTH':
        pos = select_4('positions', 'symbol', order['s'], 'position_side', order['ps'], 'user_id',
                       str(user), 'trader', str(trader))
    elif order['ps'] == 'BOTH':
        pos = select_3('positions', 'symbol', order['s'], 'user_id', str(user), 'trader', str(trader))
    return pos


def insert_pos(order, user, trader):
    # открывает позицию
    pos = select_pos(order, user, trader)
    if pos is None:
        data = [(order['s'], order['ps'], str(user), str(trader))]
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


# ------------------ Открытие ордеров ------------------
async def create_task_for_traders():
    # берём всех трейдеров
    traders = find('traders')
    for info_trader in traders:
        task = asyncio.create_task(get_order(info_trader))

    await task


async def get_order(info_trader):
    # ищем все опции , в которых есть этот трейдер
    options = select_all("options", 'traders_id', info_trader[0])
    id_options = []
    for option in options:
        id_options.append(option[0])
    trader = await AsyncClient.create(info_trader[2], info_trader[3])
    try:
        pos_mode = await trader.futures_get_position_mode()
        pos_mode = pos_mode['dualSidePosition']
        bm = BinanceSocketManager(trader)
        ts = bm.futures_socket()
        async with ts as tscm:
            while True:
                order = await tscm.recv()
                if order['e'] == 'ORDER_TRADE_UPDATE':
                    order = order['o']
                    # await trader.close_connection()
                    # если ордера нету в базе ордеров , то записываем его и открываем ордер
                    f_base = select("orders", "id", order['i'])
                    if f_base:
                        await cancels_orders(order, id_options, info_trader[0])
                    if not f_base:
                        if order['o'] == 'TRAILING_STOP_MARKET':
                            data = [(order['i'], order['s'], order['o'], order['AP'])]
                        elif float(order['ap']) > 0.0:
                            data = [(order['i'], order['s'], order['o'], order['ap'])]
                        elif float(order['p']) > 0.0:
                            data = [(order['i'], order['s'], order['o'], order['p'])]
                        else:
                            data = [(order['i'], order['s'], order['o'], order['sp'])]
                        insert("orders", data)
                        order_info = await get_open_pos(symbol=order['s'], pos_side=order['ps'],
                                                        api_key=info_trader[2], secret_key=info_trader[3])
                        # открытие ордеров
                        await create_orders(order, order_info, pos_mode, id_options, info_trader[0])
    finally:
        await trader.close_connection()


async def cancels_orders(order, id_options, trader_id):
    # проходим по пользователям и вызываем функцию для открытия ордеров
    users = find('users')
    for user in users:
        if user[10] == "on" and str(user[1]) in id_options and str(user[4]) == '1':
            client = await AsyncClient.create(user[5], user[6])
            open_ords = await client.futures_get_open_orders()
            pos = select_pos(order, user[0], trader_id)
            all_pos_and_ords = []

            if order['o'] == 'TRAILING_STOP_MARKET':
                var_order = "cr"
                var_open_order = "priceRate"
            elif float(order['ap']) > 0.0:
                var_order = "ap"
                var_open_order = "avgPrice"
            elif float(order['p']) > 0.0:
                var_order = "p"
                var_open_order = "price"
            else:
                var_order = 'sp'
                var_open_order = "stopPrice"

            if ((pos is not None and str(pos[3]) == str(trader_id)) or pos is None) and \
                    (order['X'] == "CANCELED" or order['X'] == 'EXPIRED'):
                for open_ord in open_ords:
                    # если ордер отменён или истёк,то отменяем ордер, оповещаем в боте и удаляем позицию
                    if order['s'] == open_ord['symbol'] and (
                            order['X'] == "CANCELED" or order['X'] == "EXPIRED") and \
                            float(order[var_order]) == float(open_ord[var_open_order]):
                        logger.info(str(user[5]) + ' | ' + str(user[6]))
                        logger.info('close')
                        try:
                            await send_m(user[0], order, open_ord['origQty'], "CANCEL")
                            await client.futures_cancel_order(symbol=order['s'],
                                                              orderId=open_ord['orderId'])
                        except binance.client.BinanceAPIException as e:
                            logger.error(e)

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


async def create_orders(order, order_info, pos_mode, id_options, trader_id):
    # проходим по пользователям и вызываем функцию для открытия ордеров
    users = find('users')
    for user in users:
        if user[10] == "on" and str(user[1]) in id_options and str(user[4]) == '1':
            try:
                if order['o'] == 'TRAILING_STOP_MARKET':
                    var = "AP"
                elif float(order['ap']) > 0.0:
                    var = "ap"
                elif float(order['p']) > 0.0:
                    var = "p"
                else:
                    var = 'sp'

                # если ордер не отменён и не истёк
                if order['X'] != "CANCELED" and order['X'] != 'EXPIRED':
                    client = await AsyncClient.create(user[5], user[6])
                    bal = await client.futures_account()
                    bal = float(bal['availableBalance'])
                    qty_pos = float(float(bal) * (float(user[11]) * 0.01))
                    qty_usdt = user[12]
                    # открываем ордера
                    try:
                        await new_ord(client, user[0], order, order_info, qty_pos, qty_usdt, pos_mode, trader_id, var)
                    except binance.client.BinanceAPIException as e:
                        logger.error(str(e))
                    except Exception as e:
                        logger.error(str(user[0]) + " | " + str(e))
                        continue
            except Exception as e:
                logger.error(e)
                continue


async def new_ord(client, user, order, trader_info, qty, qty_usdt, pos_mode, trader_id, context):
    logger.info('new_ord')
    # меняем мод на хедж или односторонний
    try:
        await client.futures_change_position_mode(dualSidePosition=pos_mode)
    except binance.client.BinanceAPIException as e:
        if e.code == -4059:
            pass
        else:
            logger.error(str(e.code) + " : " + str(e.message))
    client_info = await get_open_pos(symbol=order['s'], pos_side=order['ps'], client=client)
    pos = select_pos(order, user, trader_id)
    logger.info(client_info)
    logger.info(trader_info)
    logger.info(order)
    logger.info(pos)
    if (pos is not None and str(pos[3]) == str(trader_id)) or pos is None:
        shoulder = float(trader_info['leverage'])
        margin_type = trader_info["marginType"]
        # если у трейдера размер позиции больше 0
        if client_info['positionSide'] == order['ps'] and abs(float(trader_info['positionAmt'])) > 0.0:
            # --------- Зайти в позицию ---------
            # if float(client_info['positionAmt']) == 0.0 and (pos is None or (order['status'] == 'FILLED'
            #                                                                  and abs(
            #             float(trader_info['positionAmt'])) == abs(float(order['q'])))):
            if float(client_info['positionAmt']) == 0.0 and pos is None:
                # Если у трейдер есть открытая позиция, то сюда не попадает
                if (order['X'] != 'FILLED' and order['o'] != 'MARKET' and
                    abs(float(trader_info['positionAmt'])) == 0.0) or \
                        ((order['X'] == 'FILLED' or order['o'] == 'MARKET') and
                         abs(float(trader_info['positionAmt'])) - abs(float(order['q'])) == 0):
                    if order['o'] == 'MARKET':
                        count = round((qty / float(trader_info['entryPrice'])) * shoulder,
                                      num_decimal_places(str(order['q'])))
                    else:
                        count = round((qty / float(order[context])) * shoulder,
                                      num_decimal_places(str(order['q'])))
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
                if (client_info['positionSide'] == order['ps'] == "LONG" and order['S'] == "BUY") or \
                        (client_info['positionSide'] == order['ps'] == "SHORT" and
                         order['S'] == "SELL") or (client_info['positionSide'] == order['ps'] == 'BOTH'
                                                   and order['S'] == pos[1]):
                    count = float(float(abs(float(client_info['positionAmt']))) * (float(qty_usdt) * 0.01))
                    count = round(count, num_decimal_places(str(client_info['positionAmt'])))
                    # if float(count) * float(order[context]) <= 5 and not (order['status'] == 'FILLED'
                    #                                                       and abs(
                    #             float(trader_info['positionAmt'])) == abs(float(order['q']))):
                    # if float(count) * float(order[context]) <= 5:
                    #     count = 0.0
                # --------- Закрыть ЛОНГ/ШОРТ ---------
                elif (client_info['positionSide'] == order['ps'] == "LONG" and order['S'] == "SELL") \
                        or (
                        client_info['positionSide'] == order['ps'] == "SHORT" and order['S'] == "BUY") \
                        or (
                        client_info['positionSide'] == order['ps'] == 'BOTH' and order['S'] != pos[1]):
                    if order['o'] != 'MARKET' and abs(float(order['q'])) == \
                            abs(float(trader_info['positionAmt'])) and order['X'] != 'FILLED':
                        find_percent = 100.0
                    else:
                        if order['o'] == 'MARKET':
                            find_percent = round(float(order['q']) /
                                                 (abs(float(trader_info['positionAmt'])) + float(order['q']))
                                                 * 100.0, 0)
                        else:
                            find_percent = round(float(order['q']) / abs(float(trader_info['positionAmt']))
                                                 * 100.0, 0)
                    # position_info = get_open_pos(order['symbol'], order['positionSide'], client=client)
                    if abs(float(client_info['positionAmt'])) > 0:
                        count = float(float(abs(float(client_info['positionAmt']))) * (float(find_percent) * 0.01))
                    if str(count)[-1] == '5' or len(str(count)) > len(str(order['q'])):
                        nulls_ = '{:0' + str(num_decimal_places(str(count))) + '}'
                        round_number_ = float(nulls_.format(1)[0] + '.' + nulls_.format(1)[1:])
                        count = -1 * count // round_number_ * -round_number_
                    if order['cp'] is True:
                        count = abs(float(client_info['positionAmt']))
                    send_m_text = f"CLOSE {order['ps']}"
                if find_percent != 100.0 and order['R'] is False:
                    nulls_ = '{:0' + str(num_decimal_places(str(count))) + '}'
                    round_number_ = float(nulls_.format(1)[0] + '.' + nulls_.format(1)[1:])
                    count = -1 * count // round_number_ * -round_number_
                    send_m_text = f"AVERAGE {order['ps']}"
                elif find_percent != 100.0 and order['R'] is True:
                    count = round(count, num_decimal_places(str(client_info['positionAmt'])))
                try:
                    await new_orders(client, order, margin_type, shoulder, count, pos_mode)
                    await send_m(user, order, count, send_m_text)
                except binance.client.BinanceAPIException:
                    pass

        # если новый ордер (позиция трейдера равна нулю)
        elif client_info['positionSide'] == order['ps'] and order['X'] == 'NEW' and abs(
                float(client_info['positionAmt'])) == 0.0:
            if order['o'] == 'MARKET':
                count = round((qty / float(trader_info['markPrice'])) * shoulder,
                              num_decimal_places(str(order['q'])))
            else:
                count = round((qty / float(order[context])) * shoulder,
                              num_decimal_places(str(order['q'])))
            try:
                await new_orders(client, order, margin_type, shoulder, count, pos_mode)
                await send_m(user, order, count, f" {order['ps']}")
                insert_pos(order, user, trader_id)
            except binance.client.BinanceAPIException:
                pass

        # если полностью закрывается ордер
        elif client_info['positionSide'] == order['ps']:
            count = abs(float(client_info['positionAmt']))
            try:
                await new_orders(client, order, margin_type, shoulder, count, pos_mode)
                await send_m(user, order, count, f"CLOSE {client_info['positionSide']}")
            except binance.client.BinanceAPIException:
                pass
        logger.info(str(count) + " | " + str(user) + " | new_ord")
        # await client.close_connection()


async def new_orders(client, order, margin_type, shoulder, count, pos_mode):
    logger.info('new_orders')
    # процесс открытия / закрытия ордеров и позиций
    if margin_type == "cross":
        margin_type = "CROSSED"
    if margin_type == "isolated":
        margin_type = "ISOLATED"
    try:
        await client.futures_change_leverage(symbol=order['s'], leverage=int(shoulder))
        await client.futures_change_margin_type(symbol=order['s'], marginType=margin_type)
    except binance.client.BinanceAPIException as e:
        if e.code == -4028:
            await client.futures_change_margin_type(symbol=order['s'], marginType=margin_type)
        elif e.code != -4046:
            logger.error(str(count) + str(client) + str(order))
    params = {'symbol': order['s'], 'type': order['o'], 'positionSide': order['ps'],
              'side': order['S'], 'quantity': abs(count)}
    if order['o'] == "LIMIT":
        params['price'] = float(order['p'])
        params['timeInForce'] = order['f']
    if order['o'] == "MARKET":
        pass
    if order['o'] == 'STOP' or order['o'] == "TAKE_PROFIT":
        params['price'] = float(order['p'])
        params['stopPrice'] = float(order['sp'])
        params['priceProtect'] = order['pP']
    if order['o'] == "STOP_MARKET" or order['o'] == "TAKE_PROFIT_MARKET":
        params['stopPrice'] = float(order['sp'])
        params['closePosition'] = order['cp']
        params['priceProtect'] = order['pP']
        if params['closePosition'] is True:
            params.pop('quantity')
    elif order['o'] == 'TRAILING_STOP_MARKET':
        params['callbackRate'] = float(order['cr'])
        params['workingType'] = order['wt']
        params['activationPrice'] = float(order['AP'])

    if pos_mode is False and order['cp'] is False:
        params['reduceOnly'] = order['R']
    logger.info(params)
    if ('quantity' in params and params['quantity'] != 0.0) or \
            ('closePosition' in params and params['closePosition'] is True):
        try:
            await client.futures_create_order(**params)
        except binance.client.BinanceAPIException as e:
            if e.code == -2021 and 'activationPrice' in params:
                params.pop('activationPrice')
                await client.futures_create_order(**params)
            else:
                logger.error(str(e) + " | new_orders")
    if 'quantity' in params and params['quantity'] == 0.0 and \
            'closePosition' in params and params['closePosition'] is False:
        logger.debug("count - " + str(params['quantity']) + " | new_orders")
    await client.close_connection()


if __name__ == '__main__':
    try:
        logger.info('Start')
        asyncio.get_event_loop().run_until_complete(create_task_for_traders())
    except KeyboardInterrupt:
        logger.info('End')
    except aiohttp.ClientConnectorError:
        logger.debug('ConnectionError: Please turn on Wi-Fi')
