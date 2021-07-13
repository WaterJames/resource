class MyQueue:
    def __init__(self):
        self.items = []

    def push(self, value):
        self.items.append(value)

    # 弹出首个数据
    def pop(self):
        if(self.items == []):
            return 0
        else:
            return self.items.pop(0)

    def sum(self):
        total = 0.0
        if(len(self.items) == 0):
            return total
        else:
            for each_item in range (0, len(self.items)):
                total = total+ float(self.items[each_item][0])
            return total
