paths = ("data/name.basics.tsv/data.tsv",
        "data/title.akas.tsv/data.tsv",
        "data/title.basics.tsv/data.tsv",
        "data/title.crew.tsv/data.tsv",
        "data/title.episode.tsv/data.tsv",
        "data/title.principals.tsv/data.tsv",
        "data/title.ratings.tsv/data.tsv")

fields_limit_per_file = [6, 8, 9, 3, 4, 6, 3]
scan_scan = 0
mj_scan = 1
mj_mj = 2
tconst = 0
first_two_chars = 2
first_two_lines = 2

def show_info():
    print("Let's make our generator!")
    print("--------")
    print("Please choose a combo for iterator's 'merge-join' parameters")
    print("0. 'scan' - 'scan'")
    print("1. 'merge-join' - 'scan'")
    print("2. 'merge-join' - 'merge-join'")

def check_validity_of_answer(answer, down_limit, up_limit):
    while answer < down_limit or answer > up_limit:
        print("Answer: " +str(answer) + " isn't valid answer")
        print("Please enter a valid answer!")
        answer = int(input())
    return answer

def create_mj_pars(combo_answer):
    if combo_answer == scan_scan:
        print("--------")
        print("Choose 2 files for each scan's file: ")
        first_file, second_file = choose_file(True), choose_file(False)
        print("--------")
        print("Now choose fields: ")
        first_fields, second_fields = choose_field(first_file),\
                                                choose_field(second_file)
        mj_output_fields_limit = get_mj_fields_limit(first_fields)
        mj_output_fields_limit += get_mj_fields_limit(second_fields)
        fields_limit_per_file.append(mj_output_fields_limit)
        return merge_join(scan(paths[first_file]), scan(paths[second_file]),\
                                                first_fields, second_fields)
    elif combo_answer == mj_scan:
        print("We will create iterator 'merge-join' as a parameter")
        mj_par = create_mj_pars(scan_scan)
        print("----------------")
        print("Choose 1 file for scan's file: ")
        file = choose_file(True)
        print("----------------")
        print("Now choose fields: ")
        last_field = len(fields_limit_per_file) - 1
        first_fields, second_fields = choose_field(last_field),\
                                                choose_field(file)
        return merge_join(mj_par, scan(paths[file]),\
                                                first_fields, second_fields)
    elif combo_answer == mj_mj:
        print("We will create iterator 'merge-join' as the first parameter")
        mj_first_par = create_mj_pars(scan_scan)
        print("----------------")
        print("We will create iterator 'merge-join' as the second parameter")
        mj_second_par = create_mj_pars(scan_scan)
        print("----------------")
        print("Now choose fields: ")
        prelast_field, last_field = len(fields_limit_per_file) - 2,\
                                                len(fields_limit_per_file) - 1
        first_fields, second_fields = choose_field(prelast_field),\
                                                choose_field(last_field)
        return merge_join(mj_first_par, mj_second_par, first_fields,\
                                                second_fields)
    return False

def choose_file(info_flag):
    if info_flag:
        print("0. name.basics")
        print("1. title.akas")
        print("2. title.basics")
        print("3. title.crew")
        print("4. title.episode")
        print("5. title.principals")
        print("6. title.ratings")
    file = int(input())
    file = check_validity_of_answer(file, 0, 6)
    return file

def choose_field(file):
    print("Field limits for your selecting file: "
                                + str(fields_limit_per_file[file]))
    print("Choose fields with space after each number" +
                " if you want more than one field")
    fields = input()
    fields = check_fields_validity(fields, file)
    return fields

def check_fields_validity(fields, file):
    if len(fields) > 1:
        fields = fields.split()
        limit = fields_limit_per_file[file]
        for i in range(0, len(fields)):
            fields[i] = check_validity_of_answer(int(fields[i]), 0, limit)
    else:
        fields = int(fields)
    return fields

def get_mj_fields_limit(fields):
    if isinstance(fields, int):
        return 1
    else:
        return len(fields)

def scan(file):
    with open(file, encoding = "utf8") as file:
        for line in file:
            yield line.split("\t")

def merge_join(first_iterator, second_iterator, first_fields, second_fields):
    counter = 0
    equal_flag = False
    while True:
        try:
            if counter < first_two_lines:
                first_iterator_tuple = next(first_iterator)
                second_iterator_tuple = next(second_iterator)
                first_iter_join_attr = first_iterator_tuple[tconst]
                second_iter_join_attr = second_iterator_tuple[tconst]
            if (first_iter_join_attr == "nconst" or
                    first_iter_join_attr == "titleId" or
                    first_iter_join_attr == "tconst"):
                    fields = create_fields_list(first_iterator_tuple,
                                                    second_iterator_tuple,
                                                    first_fields, second_fields)
                    equal_flag = True
            if (first_iter_join_attr[:first_two_chars] == "tt" or
                                first_iter_join_attr[:first_two_chars] == "nm"):
                if int(first_iter_join_attr[first_two_chars:]) ==\
                                int(second_iter_join_attr[first_two_chars:]):
                    fields = create_fields_list(first_iterator_tuple,
                                                    second_iterator_tuple,
                                                    first_fields, second_fields)
                    equal_flag = True
                    previous_iterator_tuple = first_iterator_tuple
                    previous_join_attr = first_iterator_tuple[tconst]
                    first_iterator_tuple = next(first_iterator)
                    first_iter_join_attr = first_iterator_tuple[tconst]
                elif (int(first_iter_join_attr[first_two_chars:]) <\
                                int(second_iter_join_attr[first_two_chars:])):
                    first_iterator_tuple = next(first_iterator)
                    first_iter_join_attr = first_iterator_tuple[tconst]
                    equal_flag = False
                elif (int(first_iter_join_attr[first_two_chars:]) >\
                                int(second_iter_join_attr[first_two_chars:])):
                    second_iterator_tuple = next(second_iterator)
                    second_iter_join_attr = second_iterator_tuple[tconst]
                    equal_flag = False
                    if (int(previous_join_attr[first_two_chars:]) ==\
                                int(second_iter_join_attr[first_two_chars:])):
                        fields = create_fields_list(previous_iterator_tuple,
                                                    second_iterator_tuple,
                                                    first_fields, second_fields)
                        equal_flag = True
            if equal_flag == True:
                yield fields
            counter += 1
        except StopIteration:
            break

def create_fields_list(first_tuple, second_tuple, first_fields, second_fields):
    fields = []
    fields.append(first_tuple[tconst])
    if isinstance(first_fields, list):
        for i in range(0, len(first_fields)):
            fields.append(first_tuple[first_fields[i]])
    else:
        fields.append(first_tuple[first_fields])
    if isinstance(second_fields, list):
        for i in range(0, len(second_fields)):
            fields.append(second_tuple[second_fields[i]])
    else:
        fields.append(second_tuple[second_fields])
    return fields

show_info()
combo_answer = int(input())
combo_answer = check_validity_of_answer(combo_answer, 0, 2)
mj = create_mj_pars(combo_answer)
output = open("dataoutput.tsv", "w",  encoding = "utf-8")
for i in range(0, 100):
    tuple = next(mj)
    for field in tuple:
        output.write(field + "\t")
    output.write("\n")
output.close()
