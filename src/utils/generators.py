def batcher(data, batch_size=3):
    for i in range(0, len(data), batch_size):
        yield data[i : i + batch_size]
