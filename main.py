from helpers.merge_function import MERGE

if __name__ == '__main__':
    # define product keyword to search
    query = "Hamburger/burger"
    print("initiate search")
    # send keyword to the twitter search
    MERGE().runner(query=query)