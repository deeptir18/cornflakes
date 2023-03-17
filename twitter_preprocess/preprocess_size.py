import argparse
def insert_into_of(outfile, time, key, key_size, value_size, client_id, op,ttl):
    outfile.write(f"{time},{key},{key_size},{value_size},{client_id},{op},{ttl}\n")
def main():
    parser = argparse.ArgumentParser(
            description = "Twitter preprocess script basic argparser"
            )
    parser.add_argument("-t", "--trace",
            dest ="twitter",
            required = True,
            help = "Twitter trace to preprocess")
    parser.add_argument("-o", "--outfile",
            dest = "outfile",
            required = True,
            help = "Place to write outfile")
    parser.add_argument("-s", "--max_size",
            dest = "max_size",
            type = int,
            required = True,
            help = "Maximum packet size to truncate to")

    args = parser.parse_args()
    of = open(args.outfile, "w")
    with open(args.twitter, "r") as infile:
        for line_str in infile:
            line_str = line_str.rstrip()
            line = line_str.split(",")
            time = line[0]
            key = line[1]
            key_size = line[2]
            value_size = int(line[3])
            client_id = line[4]
            op = line[5]
            ttl = line[6]
            if value_size > args.max_size:
                num_max_size_packets = int(value_size / args.max_size)
                rem_size = int(value_size % args.max_size)
                for i in range(num_max_size_packets):
                    str_i = str(i)
                    new_key = f"{key}{str_i}"
                    new_value_size = args.max_size
                    new_key_size = len(new_key)
                    insert_into_of(
                            of,
                            time,
                            new_key,
                            str(new_key_size),
                            str(new_value_size),
                            client_id,
                            op,
                            ttl
                            )
                if rem_size > 0:
                    str_i = str(num_max_size_packets)
                    new_key = f"{key}{str_i}"
                    new_value_size = rem_size
                    new_key_size = len(new_key)
                    insert_into_of(
                            of,
                            time,
                            new_key,
                            str(new_key_size),
                            str(new_value_size),
                            client_id,
                            op,
                            ttl
                            )


            else:
                # re-insert line into output file
                insert_into_of(
                            of,
                            time,
                            key,
                            key_size,
                            str(value_size),
                            client_id,
                            op,
                            ttl)
    of.flush()
    of.close()



    
if __name__ == '__main__':
    main()
