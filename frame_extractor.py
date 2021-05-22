#!/usr/bin/env python3

import os
import tarfile
import sqlite3
import pandas as pd
from conllu import parse
from conllu import parse_incr, parse_tree_incr
import shutil
import re
import zipfile

con = sqlite3.connect('frames.db')  # подключение
cur = con.cursor()

cur.execute("""
CREATE TABLE data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sentence    TEXT,
    field    TEXT,
    form_subj    TEXT,
    lemma_subj TEXT,
    form_verb TEXT,
    lemma_verb TEXT,
    form_obj TEXT,
    lemma_obj TEXT
);""")


def read_sentences(file):
    """
    Creates a list of sentences from the corpus
    Each sentence is a string
    :param file:
    :return:
    """
    return parse_incr(open(file))


def split_rows(sentences, column_names):
    """
    Creates a list of sentence where each sentence is a list of lines
    Each line is a dictionary of columns
    :param sentences:
    :param column_names:
    :return:
    """

    new_sentences = []
    root_values = ['0', 'ROOT', 'ROOT', 'ROOT', 'ROOT', 'ROOT', '0', 'ROOT', '0', 'ROOT']
    start = [dict(zip(column_names, root_values))]
    for sentence in sentences:
        sentence = sentence.strip()
        rows = sentence.split('\n')
        sentence = [dict(zip(column_names, row.split('\t'))) for row in rows if row[0] != '#' and not isRange(row)]
        sentence = start + sentence
        new_sentences.append(sentence)
    return new_sentences


def find_verb_relation_by(sentence, center_pos, TAG):
    """
    Returns head of the sentence
    """
    for word in sentence:
        if word.get('deprel') == TAG:
            if word.get('head') == center_pos:
                break
            #deep_2_head = sentence[int(word.get('head'))].get('head')
            #if deep_2_head == center_pos:
            #    break
    else:
        return None
    return word


def isRange(id):
    pattern = re.compile("^\d+-\d+")
    return pattern.match(id)


def persist_key_to(dict_db, key):
    cur.execute("""
        INSERT INTO data (sentence, field, form_subj, 
        lemma_subj, form_verb, lemma_verb, 
        form_obj, lemma_obj) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, key)
    dict_db.commit()


def tupleize_by_SVO(dict_db, corpus, text_corpus):
    '''Функция находит подлежащее, сказуемое и прямое дополнение
    создает кортеж'''

    TAG_SUBJECT = 'nsubj'.lower()
    TAG_OBJECT = 'obj'.lower()
    for sentence, text in zip(corpus, text_corpus):
        for word in sentence:
            if word.get('deprel') == TAG_SUBJECT:
                verb_pos = word.get('head')
                found_object = find_verb_relation_by(sentence.copy(), verb_pos, TAG_OBJECT)
                if found_object != None:  # Checking wether there is an object

                    form_verb = sentence[int(word.get('head'))].get('form').lower()
                    lemma_verb = sentence[int(word.get('head'))].get('lemma').lower()

                    if lemma_verb.endswith('прятать') or lemma_verb.endswith('прятывать'):
                        field = 'прятать'
                    elif lemma_verb == 'скрыть' or lemma_verb == 'скрывать':
                        field = 'скрывать'
                    elif lemma_verb.endswith('менять') and lemma_verb != 'применять' \
                         and lemma_verb != 'отменять':
                        field = 'менять'
                    elif (lemma_verb.endswith('искать') or lemma_verb.endswith('ыскать') \
                          and not lemma_verb.endswith('тискать') and not lemma_verb.endswith('рыскать')) \
                          or lemma_verb.endswith('ыскивать'):
                        field = 'искать'
                    elif lemma_verb.endswith('найти') or lemma_verb.endswith('находить'):
                        field = 'находить'
                    else:
                        continue
                    
                    # Get forms and lemmas    
                    form_subject = word.get('form').lower()
                    lemma_subject = word.get('lemma').lower()

                    form_object = found_object.get('form').lower()
                    lemma_object = found_object.get('lemma').lower()
                    
					# Write to DB
                    persist_key_to(dict_db, (text, field, form_subject, lemma_subject, form_verb, lemma_verb, 
                                             form_object, lemma_object))


print('Taiga unpacked!')

files = [f for f in os.listdir('/home/aaksenova/term2021') \
         if os.path.isfile(os.path.join('/home/aaksenova/term2021', f)) and f.endswith('tar.gz') \
         or f.endswith('zip')]
         
         
for f in files:
    path = '/home/aaksenova/term2021'
    if f.endswith('gz'):  # there are not tar files
        tf = tarfile.open(os.path.join(path, f))
        tf.extractall(path)
        if not f.split('.')[0] in os.listdir('/home/aaksenova/term2021'):  # There are two types of directories
            path = '/home/aaksenova/term2021/home/tsha'
    else:
        with zipfile.ZipFile(os.path.join(path, f), 'r') as zip_f:  # For zip files
            zip_f.extractall('/home/aaksenova/term2021') # /term2021/proza_ru/home/tsha/proza_ru/tagged_texts
        path = '/'.join(['/home/aaksenova/term2021', f.split('.')[0], 'home/tsha'])
    tagged = os.path.join(path, f.split('.')[0], 'tagged_texts')
    print(f, 'unpacked!')
    for text_file in os.listdir(tagged):
        if text_file.endswith('.txt'):
            column_names = ['id', 'form', 'lemma', 'cpostag', 'postag', 'feats', 'head', 'deprel', 'phead', 'pdeprel']
            
            for sentences in parse_incr(open(os.path.join(tagged, text_file))):
                text_corpus = [sentences.metadata['text']]
                sentences = [sentences.serialize()]
                try:
                    formatted_corpus = split_rows(sentences, column_names)
                except:
                    continue
                tupleize_by_SVO(con, formatted_corpus, text_corpus)

    shutil.rmtree(os.path.join(path, f.split('.')[0]))  # Remove the directory
    print(f, 'finished!')