#coding = 'utf-8'

from sklearn.feature_extraction.text import CountVectorizer
from sklearn.decomposition import LatentDirichletAllocation
import config
import jieba
import re
import numpy as np

class Text_cluster(object):
    def __init__(self):
        self.origin_path, self.db = config.origin_path()
        f = open(self.origin_path + '/References/stop.txt').readlines()
        self.stop = [re.sub(r'\n', '', str(i)) for i in f]
        self.ch_list = ['36氪', '新浪科技', '网易智能', '亿欧', '雷锋网', '36Kr', '网易科技']

    def gen_corpus(self, text_list):
        jieba.load_userdict(self.origin_path + '/References/dict.txt')
        text_out = []
        for i in text_list:
            cut_ = jieba.lcut(i)
            out_ = ' '.join([i for i in cut_ if i not in self.stop])
            text_out.append(out_)

        return text_out

    def LDA(self, text_list, corpus_list, n_topics = 2, learning_offset = 50., random_state=0):
        Vector = CountVectorizer()
        TF = Vector.fit_transform(corpus_list)
        lda = LatentDirichletAllocation(n_topics=n_topics,
                                        learning_offset=learning_offset,
                                        random_state=random_state,
                                        max_iter=10)
        docres = lda.fit_transform(TF)
        doc_clusters = np.argmax(docres, axis=1)
        result_c = list(zip(list(doc_clusters), text_list))
        result_o = sorted(result_c,key= lambda x:x[1])
        cluster_dict = {}
        for i in result_o:
            cluster_dict[i[0]] = [x[1] for x in result_o if x[0] == i[0]]
        for i in cluster_dict.keys():
            print('type: {}'.format(i))
            print(cluster_dict[i])
            print('=============')