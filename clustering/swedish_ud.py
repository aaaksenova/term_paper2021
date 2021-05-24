!pip3 install ufal.udpipe
from ufal.udpipe import Model, Pipeline
UDPIPE_MODEL_FN = "model_ru.udpipe"
!wget -O {UDPIPE_MODEL_FN} https://github.com/jwijffels/udpipe.models.ud.2.0/blob/master/inst/udpipe-ud-2.0-170801/swedish-ud-2.0-170801.udpipe?raw=true
model = Model.load(UDPIPE_MODEL_FN)
with open('swe_wikipedia_2016_1M-sentences.txt') as fh:
    text = fh.read()
text = text.split('\n')
text = [i.split('\t')[-1] for i in text]
with open('swe_ud.txt', 'w') as fw:
    for sent in text:
        text_ud = pipeline.process(sent)
        fw.write(text_ud)