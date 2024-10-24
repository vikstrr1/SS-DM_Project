from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common import Types
from datetime import datetime
import csv

class TickEvent:
    def __init__(self, symbol, sec_type, last_price, trading_time, trading_date):
        self.symbol = symbol
        self.sec_type = sec_type
        self.last_price = float(last_price)
        self.trading_time = trading_time
        self.trading_date = trading_date

def parse_line(line):
    fields = line.split(',')
    if len(fields) < 28:
        return None
    symbol = fields[0]
    sec_type = fields[1]
    last_price = fields[21]
    trading_time = fields[24]
    trading_date = fields[27]
    return TickEvent(symbol, sec_type, last_price, trading_time, trading_date)

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    
    tick_data = env.read_text_file("/opt/flink/data/debs2022-gc-trading-day-*.csv") \
        .map(lambda line: parse_line(line), output_type=Types.PICKLED_BYTE_ARRAY())
    
    tick_data = tick_data.filter(lambda event: event is not None)
    
    # Add logic for calculating EMA and detecting breakout patterns here
    
    tick_data.print()  # For testing; remove in production

    env.execute("Flink Trading Data Processor")

if __name__ == '__main__':
    main()
