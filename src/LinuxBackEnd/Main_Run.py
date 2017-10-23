#!/usr/local/bin/python
# coding: utf-8

__metclass__ = type

from C_Algorithms_BestMACDPattern import C_BestMACDPattern, C_MACD_Signal_Calculation, C_MACD_Ending_Profit_Calculation


def main():
    # cal_specific_pattern()
    caL_all_pattern()


def caL_all_pattern():
    # gama is parameter to MACD(MAX-P) when saleing stock
    # Using loops to find out the best MACD parameters.
    MACDPattern = C_BestMACDPattern()
    MACD_Trading_Signal_Cal = C_MACD_Signal_Calculation()
    MACD_Ending_Profit_Cal = C_MACD_Ending_Profit_Calculation()
    # gama = [0.8, 0.7, 0.65, 0.6, 0.4, 0.45, 0.3]
    quo = [0.5, 0.6, 0.7, 0.75, 0.8, 0.9]
    # beta = [0.6, 0.65, 0.7, 0.75, 0.8, 0.85, 0.9]
    beta = [0.2]
    quo = [0.7]
    gama = [0.3]
    for each_quo in quo:
        for each_ga in gama:
            for each_beta in beta:
                MACD_Trading_Signal_Cal._MACD_trading_signals(period="m30", stock_code="sz002310", quo=each_quo,
                                                              ga=each_ga, beta=each_beta)
                # MACD_Ending_Profit_Cal._MACD_ending_profits(period='m30', stock_code='sz002310')
                # MACDPattern._save_MACD_best_pattern(period='m30')
                #MACDPattern._get_best_pattern('sz002310')


def cal_specific_pattern():
    MACDPattern = C_BestMACDPattern()
    MACD_Trading_Signal_Cal = C_MACD_Signal_Calculation()
    MACD_Ending_Profit_Cal = C_MACD_Ending_Profit_Calculation()
    gama = [0.3]
    quo = [0.7]
    # beta = [0.1, 0.15, 0.2, 0.25,  0.3, 0.35, 0.4, 0.5, 0.6, 0.65, 0.7, 0.75, 0.8, 0.85, 0.9]
    beta = [0.2]
    pattern_signal = ["2115", ]
    pattern_profit = [["2115"]]
    for each_quo in quo:
        for each_ga in gama:
            for each_beta in beta:
                MACD_Trading_Signal_Cal._single_pattern_signal_cal(MACD_pattern=pattern_signal, period="m30",
                                                                   stock_code="sz002310", quo=each_quo, ga=each_ga,
                                                                   beta=each_beta, simplified=True)
                MACD_Ending_Profit_Cal._single_pattern_ending_profit_cal(MACD_pattern=pattern_profit, period='m30',
                                                                         stock_code='sz002310')
                # MACDPattern._save_MACD_best_pattern(period='m30')
                MACDPattern._get_best_pattern('sz002310')


if __name__ == '__main__':
    main()
    # cProfile.run('main()', filename="my.profile")
    # import pstats

    # p = pstats.Stats("my.profile")
    # p.strip_dirs().sort_stats("time").print_stats(15)
