import pickle

def save(dictionary, file_path):
    with open(file_path, 'wb') as pickle_file:
        pickle.dump(dictionary, pickle_file)

def load(file_path):
    with open(file_path, 'rb') as pickle_file:
        return pickle.load(pickle_file)
