from data_cleansing.BT.BT import StartBt
import sys

if __name__ == '__main__':
    try:
        cpu_count = int(sys.argv[1])
    except:
        cpu_count = 1
    start_bt = StartBt()
    start_bt.load_source_data(cpu_count)
