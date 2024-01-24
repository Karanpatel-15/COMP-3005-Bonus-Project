import pandas as panda
import re

def parse_relations(fname):
    relations = {}
    file = open(fname, 'r')
    line = file.readline()
    while line: # Read all lines from relations file
        relation_name = line.split(' ')[0] # Get relation name from first word
        relations[relation_name] = parse_relation_by_line(line) # Parse relation and add to dictionary
        line = file.readline()
    file.close()
    return relations

def parse_queries(fname):
    file = open(fname, 'r')
    lines = file.readlines() # Read all lines from query file
    file.close()
    return lines

def parse_relation_by_line(relation_str):
    try:
        cols = re.search(r'\((.*?)\)', relation_str).group(1).split(', ') # Get columns from between parentheses
        rows_str = re.search(r'=\s*\{(.*?)\}', relation_str).group(1) # Get rows from between curly braces
        rows = [tuple(row.split(', ')) for row in rows_str.split('; ')] # Split rows by semicolon and then by comma

        df = panda.DataFrame(rows, columns=cols) # Create dataframe from rows and columns

        for col in df.columns:
            try:
                df[col] = panda.to_numeric(df[col]) # Convert columns to numeric if possible
            except:
                pass
        return df
    except Exception as e: # Catch any errors and print them
        print(f"Error parsing relation: {relation_str}")
        print(f"Error message: {e}")
        raise

def handle_query(query, relations):
    operation, params = query.split(' ', 1) # Split query into operation and parameters
    
    match operation: # Match operation to a case
        case 'select': 
            cond, relation_name = re.match(r'(.*)\((.*)\)', params).groups()
            return relations[relation_name].query(cond) # Query relation with condition
        
        case 'project':
            cols, relation_name = re.match(r'(.*)\((.*)\)', params).groups()
            return relations[relation_name][cols.split(', ')] # Select columns from relation
        
        case 'join':
            relation_names, on = re.match(r'(.*) on (.*)', params).groups()
            rel1_name, rel2_name = relation_names.split(', ')
            return panda.merge(relations[rel1_name], relations[rel2_name], on=on) # Join relations on column
        
        case 'union':
            rel1_name, rel2_name = params.split(', ')
            return panda.concat([relations[rel1_name], relations[rel2_name]]).drop_duplicates().reset_index(drop=True) # Concatenate relations and drop duplicates
        
        case 'intersect':
            rel1_name, rel2_name = params.split(', ')
            return panda.merge(relations[rel1_name], relations[rel2_name]) # Merge relations
        
        case 'difference':
            rel1_name, rel2_name = params.split(', ')
            return panda.concat([relations[rel1_name], relations[rel2_name], relations[rel2_name]]).drop_duplicates(keep=False) # Concatenate relations and drop duplicates

        case _:
            raise NotImplementedError("Operation not supported") # Raise error if operation is not supported

def main():
    relations = parse_relations('Relations.txt') # Parse relations from file
    queries = parse_queries('Queries.txt') # Parse queries from file

    for query in queries: # Execute each query
        query = query.strip()
        if query:
            try:
                result = handle_query(query, relations) # Handle query
                print(f"Query is {query}\nResult is \n{result}\n") # Print query and result
            except Exception as e:
                print(f"Error executing query '{query}': {e}") # Print error if query fails

if __name__ == "__main__":
    main()
