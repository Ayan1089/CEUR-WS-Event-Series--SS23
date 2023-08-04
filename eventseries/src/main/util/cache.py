import pickle


class Cache:
    def __init__(self):
        self.cache = {}

    def get(self, key):
        if key in self.cache:
            return self.cache[key]
        else:
            return None

    def set(self, key, value):
        self.cache[key] = value

    def is_empty(self):
        if len(self.cache) == 0:
            return True
        else:
            return False

    def save_cache(self, filename):
        with open(filename, "wb") as file:
            pickle.dump(self.cache, file)

    def load_cache(self, filename):
        try:
            with open(filename, "rb") as file:
                self.cache = pickle.load(file)
        except (FileNotFoundError, pickle.UnpicklingError):
            pass

    def print_cache_data(self, filename):
        with open(filename, "rb") as file:
            cache_data = pickle.load(file)
