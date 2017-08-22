# from pymongo import MongoClient
# from bson.code import Code
# from nam_basic import create_connect_to_mongo

def nam():
    db = create_connect_to_mongo()


    mapper = Code("""
        function() {
                      for (var key in this) { emit(key, null); }
                   }
    """)
    reducer = Code("""
        function(key, stuff) { return null; }
    """)

    distinctThingFields = db.FbGroups.map_reduce(mapper, reducer
        , out = {'inline' : 1}
        , full_response = True)
    ## do something with distinctThingFields['results']


class Index_doc():
    '''
    class để so sánh số thứ tự của các index
    '''
    def __init__(self, number):
        self.number = number
        # self.childrent = childrent
        # self.parent = parent
        # để compare các index với nhau, ta dùng chỉ số level, sau đó dùng để chỉ số index . VÍ dụ:  các số la mã level 0, số 1 là level 0, 1.1 là level 2
        # order để so sánh những index cùng level với nhau.
        # self.level,self.order = self.level()
        # __repr__ = __str__
    def __str__(self):
        return self.number
    def __repr__(self):
        return self.number
    def __lt__(self, other):
        '''
        ta so sánh lần lượt, các chỉ số được ngăn cách bởi dấu chấm
        :param other: 
        :return: 
        '''
        split_number_self = self.split_by
        split_number_other = other.split_by
        # split_number_self = self.split_by
        # split_number_other = other.split_by
        len_self = len(split_number_self)
        len_other = len(split_number_other)
        len_max = max([len_other,len_self])
        split_number_self.extend([0] * (len_max - len(self.split_number_self)))
        split_number_other.extend([0] * (len_max - len(self.split_number_other)))
        for i in range(len_max):
            if split_number_self[i] != split_number_other[i]:
                return split_number_self[i] < split_number_other[i]

    def __gt__(self, other):
        '''
        ta so sánh lần lượt, các chỉ số được ngăn cách bởi dấu chấm
        :param other: 
        :return: 
        '''
        split_number_self = self.split_by
        split_number_other = other.split_by
        # split_number_self = self.split_by
        # split_number_other = other.split_by
        len_self = len(split_number_self)
        len_other = len(split_number_other)
        len_max = max([len_other, len_self])
        split_number_self.extend([0] * (len_max - len(self.split_number_self)))
        split_number_other.extend([0] * (len_max - len(self.split_number_other)))
        for i in range(len_max):
            if split_number_self[i] != split_number_other[i]:
                return split_number_self[i] > split_number_other[i]

    def split_by(self):
        level_regex = re.compile('[1-9]\.')
        if re.search(level_regex, number) is not None:
            split_number = number.split('.')
            split_number = [int(x) for x in split_number if x.isdigit()]
            return split_number


if __name__ =='__main__':
    x = Index_doc('1.1.1')
    y = Index_doc('1.2.1')
    y = Index_doc('1.2.1')