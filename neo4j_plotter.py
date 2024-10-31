import time
from neo4j import GraphDatabase
import matplotlib.pyplot as plt
import json

def run_query(session, query):
    start_time = time.time()
    
    with session.begin_transaction() as tx:
        result = tx.run(query)
        result.consume()
    
    end_time = time.time()
    return end_time - start_time

def run_and_plot_queries():
    driver = GraphDatabase.driver(
        "bolt://localhost:7687", auth=("neo4j", "password")
    )
    
    with driver.session() as session:
        with open('second_modelation_neo4j_queries.json', 'r') as file:
            queries = json.load(file)
        
        for query_info in queries:
            query_number = query_info['query_number']
            query = query_info['query']
            description = query_info['description']
            
            times = []

            for i in range(100):
                time_taken = run_query(session, query)
                times.append(time_taken)
                print(f"Query {query_number} - Run {i + 1}: {time_taken} seconds")

            average_time = sum(times) / len(times)

            plt.figure(figsize=(10, 6))
            plt.plot(times, label="Query Execution Time")
            plt.axhline(y=average_time, color='r', linestyle='--', label=f'Average Time: {average_time:.4f} seconds')
            plt.xlabel('Run Number')
            plt.ylabel('Time (seconds)')
            plt.title(f'Query {query_number}: {description} Execution Time Over 100 Runs')
            plt.legend()
            plt.grid(True)
            
            plt.savefig(f'neo4j_query_{query_number}_execution_times_noindex_Charles_Leclerc.png')

    driver.close()

run_and_plot_queries()
