import sys
sys.path.append("C:\\Users\\Omar\\PycharmProjects\\data_cleansing")
from data_cleansing.BT.BT import StartBt
from data_cleansing.DQ.DQ import StartDQ

if __name__ == '__main__':
    try:
        process_no = int(sys.argv[1])
    except:
        process_no = -1

    try:
        BT = int(sys.argv[2])
    except:
        BT = 0

    try:
        DQ = int(sys.argv[3])
    except:
        DQ = 0

    try:
        cpu_num_workers = int(sys.argv[4])
    except:
        cpu_num_workers = -1

    if BT == 1:
        # BT_time = datetime.datetime.now()
        bt = StartBt()
        bt.start_bt(process_no, cpu_num_workers)
        # print('----------------     BT_time:', datetime.datetime.now() - BT_time, '      ----------------')
    if DQ == 1:
        # DQ_time = datetime.datetime.now()
        start_dq = StartDQ()
        start_dq.start_dq()
        # print('----------------     DQ_time:', datetime.datetime.now() - DQ_time, '      ----------------')
