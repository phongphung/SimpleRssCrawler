from WebCrawler import WebCrawlerWraper
import multiprocessing as mp
import pandas as pd
import numpy
import time

fn = 'rss_result.csv'


def chunks(l, n):
    return list(numpy.array_split(numpy.array(l), n))


def worker(url_list, q):
    crawler = WebCrawlerWraper(url_list, 2, 60)
    crawler.crawl()
    res = crawler.data.to_csv()
    q.put(res)


def listener(q):
    """ Get message, then write """
    f = open(fn, 'w')
    while 1:
        m = q.get()
        if m == 'kill':
            f.write('killed')
            break
        f.write(str(m) + '\n')
        f.flush()
    f.close()


def main():
    processes = 8
    manager = mp.Manager()
    q = manager.Queue()
    pool = mp.Pool(processes)

    url_list = list(pd.read_excel('backup_noovell.xlsx')['url'])
    chunks_list = chunks(url_list, processes)

    # put listener to work
    watcher = pool.apply_async(listener, (q, ))

    # get worker to work
    jobs = []
    for i in chunks_list:
        job = pool.apply_async(worker, (i, q))
        jobs.append(job)

    # collect results
    for job in jobs:
        job.get()

    # kill listener
    q.put('kill')
    pool.close()


if __name__ == '__main__':
    main()