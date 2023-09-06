import pickle


class Cache:
    def __init__(self):
        self.volume_cache = {}
        self.absent_volume_cache = []

    def get(self, key):
        if key in self.volume_cache:
            return self.volume_cache[key]
        else:
            return None

    def set(self, key, value):
        self.volume_cache[key] = value

    def is_empty(self):
        if len(self.volume_cache) == 0:
            return True
        else:
            return False

    def save_volume_cache(self, filename):
        try:
            with open(filename, "rb") as file:
                volume_cache = pickle.load(file)
        except (FileNotFoundError, pickle.UnpicklingError):
            pass
        if len(volume_cache) < len(self.volume_cache):
            with open(filename, "wb") as file:
                pickle.dump(self.volume_cache, file)
        else:
            return

    def load_volume_cache(self, filename):
        try:
            with open(filename, "rb") as file:
                self.volume_cache = pickle.load(file)
        except (FileNotFoundError, pickle.UnpicklingError):
            pass

    def save_absent_volume_cache(self, filename):
        try:
            with open(filename, "rb") as file:
                volume_cache = pickle.load(file)
        except (FileNotFoundError, pickle.UnpicklingError):
            volume_cache = []
            pass
        if len(volume_cache) < len(self.absent_volume_cache):
            with open(filename, "wb") as file:
                pickle.dump(self.absent_volume_cache, file)

    def load_absent_volume_cache(self, filename):
        try:
            with open(filename, "rb") as file:
                self.absent_volume_cache = pickle.load(file)
        except (FileNotFoundError, pickle.UnpicklingError):
            pass

    def print_cache_data(self, filename):
        with open(filename, "rb") as file:
            cache_data = pickle.load(file)
