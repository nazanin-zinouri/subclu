"""
CLI utility to get embeddings for a given input type
It's meant to work for different input types:
- subreddit metadata
- posts (with or without comments text as a single text file)
- comments
By default we'll get one embedding for each individual row.

In a separate step we'll take care of combining/aggregating embedddings.
For example:
- post_vector = weighted_avg([post_vector, comment_vectors])

The code to do this lives in: subclu2.get_embeddings.vectorize_text_tf.py
Need to figure out how to call that from this top-level folder that kubeflow expects.
"""
