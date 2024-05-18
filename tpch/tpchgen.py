import argparse

def generate_tpch_csv(scale_factor: int, partitions: int):
    if partitions == 1:
        command = ["./dbgen", "-f", "-s", scale_factor]
        print(command)
    else:
        for i in range(1, partitions+1):
            command = ["./dbgen", "-f", "-s", scale_factor, "-C", partitions, "-S", i]
            print(command)

if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('--scale-factor', type=int, help='The scale factor')
    arg_parser.add_argument('--partitions', type=int, help='The number of partitions')
    args = arg_parser.parse_args()
    generate_tpch_csv(args.scale_factor, args.partitions)