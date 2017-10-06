from WebCrawler import WebCrawlerWraper
import multiprocessing as mp
import pandas as pd
import numpy
import logging

fn = 'tue_rss.csv'


def chunks(l, n):
    return list(numpy.array_split(numpy.array(l), n))


def worker(url_list, q, backup):
    try:
        crawler = WebCrawlerWraper(url_list, 2, 400, backup)
        crawler.crawl()
        res = crawler.data.to_csv()
        q.put(res)
    except Exception as e:
        crawler.data.to_csv('error_' + str(backup) + '.csv')
        crawler.control.to_csv('error_control_' + str(backup) + '.csv')
        raise e


def listener(q):
    """ Get message, then write """
    f = open(fn, 'a')
    while 1:
        m = q.get()
        if m == 'kill':
            # f.write('killed')
            break
        f.write(str(m) + '\n')
        f.flush()
    f.close()


def main():
    LOG_FILENAME = 'tue_RSS.log'
    logging.basicConfig(filename=LOG_FILENAME, level=logging.DEBUG)

    try:
        processes = 12
        manager = mp.Manager()
        q = manager.Queue()
        pool = mp.Pool(processes)

        url_list = list(pd.read_csv('tue_RSS.csv')['url'])
        chunks_list = chunks(url_list, processes)

        # put listener to work
        watcher = pool.apply_async(listener, (q, ))

        # get worker to work
        jobs = []
        count = 0
        for i in chunks_list:
            job = pool.apply_async(worker, (i, q, 'tue_RSS'))
            jobs.append(job)
            count += 1

        # collect results
        for job in jobs:
            job.get()

        # kill listener
        q.put('kill')
        pool.close()
    except Exception as e:
        logging.exception(e)


if __name__ == '__main__':
    main()
