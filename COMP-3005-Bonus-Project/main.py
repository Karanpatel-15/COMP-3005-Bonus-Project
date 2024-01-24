import pandas as panda
import re

def parse_relations(fname):
    relations = {}
    with open(fname, 'r') as file:
        for line in file:
            relation_name = line.split(' ')[0]
            relations[relation_name] = parse_relation_by_line(line)
    return relations

def parse_queries(fname):
    with open(fname, 'r') as file:
        return file.readlines()

def parse_relation_by_line(relation_str):
    try:
        cols = re.search(r'\((.*?)\)', relation_str).group(1).split(', ')
        rows_str = re.search(r'=\s*\{(.*?)\}', relation_str).group(1)
        rows = [tuple(row.split(', ')) for row in rows_str.split('; ')]

        df = panda.DataFrame(rows, columns=cols)

        for col in df.columns:
            try:
                df[col] = panda.to_numeric(df[col])
            except:
                pass
        return df
    except Exception as e:
        print(f"Error parsing relation: {relation_str}")
        print(f"Error message: {e}")
        raise

def process_select(dataframe, cond):
    return dataframe.query(cond)

def process_union(df1, df2):
    return panda.concat([df1, df2]).drop_duplicates().reset_index(drop=True)

def process_difference(df1, df2):
    return panda.concat([df1, df2, df2]).drop_duplicates(keep=False)

def process_project(dataframe, cols):
    return dataframe[cols.split(', ')]

def process_join(df1, df2, on):
    return panda.merge(df1, df2, on=on)

def process_intersection(df1, df2):
    return panda.merge(df1, df2)


def handle_query(query, relations):
    operation, params = query.split(' ', 1)
    
    match operation:
        case 'select':
            cond, relation_name = re.match(r'(.*)\((.*)\)', params).groups()
            return process_select(relations[relation_name], cond)
        
        case 'project':
            cols, relation_name = re.match(r'(.*)\((.*)\)', params).groups()
            return process_project(relations[relation_name], cols)
        
        case 'join':
            relation_names, on = re.match(r'(.*) on (.*)', params).groups()
            rel1_name, rel2_name = relation_names.split(', ')
            return process_join(relations[rel1_name], relations[rel2_name], on)
        
        case 'union':
            rel1_name, rel2_name = params.split(', ')
            return process_union(relations[rel1_name], relations[rel2_name])
        
        case 'intersect':
            rel1_name, rel2_name = params.split(', ')
            return process_intersection(relations[rel1_name], relations[rel2_name])
        
        case 'difference':
            rel1_name, rel2_name = params.split(', ')
            return process_difference(relations[rel1_name], relations[rel2_name])
        
        case _:
            raise NotImplementedError("Operation not supported")

def main():
    relations = parse_relations('relations.txt')
    queries = parse_queries('queries.txt')

    for query in queries:
        query = query.strip()
        if query:
            try:
                result = handle_query(query, relations)
                print(f"Query: {query}\nResult:\n{result}\n")
            except Exception as e:
                print(f"Error executing query '{query}': {e}")

if __name__ == "__main__":
    main()
