with open('title.basics.tsv', 'r', encoding='utf-8') as ipt:
    with open('long_test.txt', 'w') as out:
        for x in range(10000):
            line = ipt.readline()
            out.write(line)
