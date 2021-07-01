"""
Utilities to standardize loading models from tensorflow's modelhub

TF Hub links
- https://tfhub.dev/google/universal-sentence-encoder-multilingual-large/3

Examples:
- https://colab.research.google.com/github/tensorflow/hub/blob/master/examples/colab/cross_lingual_similarity_with_tf_hub_multilingual_universal_encoder.ipynb
- https://colab.research.google.com/github/tensorflow/hub/blob/master/examples/colab/semantic_similarity_with_tf_hub_universal_encoder.ipynb

"""

D_MODELS_TF_HUB = {
    'use_multilingual_large': "https://tfhub.dev/google/universal-sentence-encoder-multilingual-large/3",
    'use_multilingual': "https://tfhub.dev/google/universal-sentence-encoder-multilingual/3",

}
