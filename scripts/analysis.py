import pandas as pd


def result_analysis():
    tasks = ['fb']
    indexs = ['BTree', 'DynamicPGM', 'LIPP', 'HybridPGMLIPP']
    # Create dictionaries to store throughput data for each index
    lookuponly_throughput = {}
    insertlookup_throughput = {}
    insertlookup_mix1_throughput = {}
    insertlookup_mix2_throughput = {}
    
    # Create dictionaries to store size data for each index
    lookuponly_size = {}
    insertlookup_size = {}
    insertlookup_mix1_size = {}
    insertlookup_mix2_size = {}
    
    for index in indexs:
        lookuponly_throughput[index] = {}
        insertlookup_throughput[index] = {"lookup": {}, "insert": {}}
        insertlookup_mix1_throughput[index] = {}
        insertlookup_mix2_throughput[index] = {}
        
        lookuponly_size[index] = {}
        insertlookup_size[index] = {}
        insertlookup_mix1_size[index] = {}
        insertlookup_mix2_size[index] = {}
    
    for task in tasks:
        full_task_name = f"{task}_100M_public_uint64"
        lookup_only_results = pd.read_csv(f"results/{full_task_name}_ops_2M_0.000000rq_0.500000nl_0.000000i_results_table.csv")
        insert_lookup_results = pd.read_csv(f"results/{full_task_name}_ops_2M_0.000000rq_0.500000nl_0.500000i_0m_results_table.csv")
        insert_lookup_mix_1_results = pd.read_csv(f"results/{full_task_name}_ops_2M_0.000000rq_0.500000nl_0.100000i_0m_mix_results_table.csv")
        insert_lookup_mix_2_results = pd.read_csv(f"results/{full_task_name}_ops_2M_0.000000rq_0.500000nl_0.900000i_0m_mix_results_table.csv")
        
        # Store labels for each index
        lookup_labels = {}
        insert_labels = {}
        mix1_labels = {}
        mix2_labels = {}
        
        for index in indexs:
            try:
                # Lookup only
                lookup_only_result = lookup_only_results[lookup_only_results['index_name'] == index]
                if not lookup_only_result.empty:
                    # Calculate mean throughput for each row
                    lookup_only_result['mean_throughput'] = lookup_only_result[['lookup_throughput_mops1', 'lookup_throughput_mops2', 'lookup_throughput_mops3']].mean(axis=1)
                    # Get row with max throughput
                    max_row = lookup_only_result.loc[lookup_only_result['mean_throughput'].idxmax()]
                    lookuponly_throughput[index][task] = max_row['mean_throughput']
                    lookuponly_size[index][task] = max_row['index_size_bytes']
                    lookup_labels[index] = f"{index}\n({max_row['search_method']}, {max_row['value']})"
            
            except:
                pass
            
            try:
                # Insert lookup
                insert_lookup_result = insert_lookup_results[insert_lookup_results['index_name'] == index]
                if not insert_lookup_result.empty:
                    # Calculate mean throughput for each row
                    insert_lookup_result['mean_lookup_throughput'] = insert_lookup_result[['lookup_throughput_mops1', 'lookup_throughput_mops2', 'lookup_throughput_mops3']].mean(axis=1)
                    insert_lookup_result['mean_insert_throughput'] = insert_lookup_result[['insert_throughput_mops1', 'insert_throughput_mops2', 'insert_throughput_mops3']].mean(axis=1)
                    # Get row with max lookup throughput
                    max_row = insert_lookup_result.loc[insert_lookup_result['mean_lookup_throughput'].idxmax()]
                    insertlookup_throughput[index]['lookup'][task] = max_row['mean_lookup_throughput']
                    insertlookup_throughput[index]['insert'][task] = max_row['mean_insert_throughput']
                    insertlookup_size[index][task] = max_row['index_size_bytes']
                    insert_labels[index] = f"{index}\n({max_row['search_method']}, {max_row['value']})"
            
            except:
                pass
            
            try:
                # Mixed workload 1
                insert_lookup_mix_1_result = insert_lookup_mix_1_results[insert_lookup_mix_1_results['index_name'] == index]
                if not insert_lookup_mix_1_result.empty:
                    insert_lookup_mix_1_result['mean_throughput'] = insert_lookup_mix_1_result[['mixed_throughput_mops1', 'mixed_throughput_mops2', 'mixed_throughput_mops3']].mean(axis=1)
                    max_row = insert_lookup_mix_1_result.loc[insert_lookup_mix_1_result['mean_throughput'].idxmax()]
                    insertlookup_mix1_throughput[index][task] = max_row['mean_throughput']
                    insertlookup_mix1_size[index][task] = max_row['index_size_bytes']
                    mix1_labels[index] = f"{index}\n({max_row['search_method']}, {max_row['value']})"
            
            except:
                pass
            
            try:
                # Mixed workload 2
                insert_lookup_mix_2_result = insert_lookup_mix_2_results[insert_lookup_mix_2_results['index_name'] == index]
                if not insert_lookup_mix_2_result.empty:
                    insert_lookup_mix_2_result['mean_throughput'] = insert_lookup_mix_2_result[['mixed_throughput_mops1', 'mixed_throughput_mops2', 'mixed_throughput_mops3']].mean(axis=1)
                    max_row = insert_lookup_mix_2_result.loc[insert_lookup_mix_2_result['mean_throughput'].idxmax()]
                    insertlookup_mix2_throughput[index][task] = max_row['mean_throughput']
                    insertlookup_mix2_size[index][task] = max_row['index_size_bytes']
                    mix2_labels[index] = f"{index}\n({max_row['search_method']}, {max_row['value']})"
            
            except:
                pass
    
    # Plot throughput results
    import matplotlib.pyplot as plt
    fig, axs = plt.subplots(2, 2, figsize=(15, 12))
    axs = axs.flatten()
    
    bar_width = 0.15
    index = range(len(indexs))
    colors = ['blue', 'green', 'red', 'orange', 'purple']
    
    # 1. Plot lookup-only throughput
    ax = axs[0]
    for i, task in enumerate(tasks):
        task_data = []
        for idx in indexs:
            task_data.append(lookuponly_throughput[idx].get(task, 0))
        ax.bar([x + i*bar_width for x in index], task_data, bar_width, label=task, color=colors[i])
        
    ax.set_title('Lookup-only Throughput')
    ax.set_ylabel('Throughput (Mops/s)')
    ax.set_xticks([x + bar_width*1.5 for x in index])
    ax.set_xticklabels([lookup_labels.get(idx, idx) for idx in indexs], rotation=45, ha='right')
    ax.legend()
    
    # 2. Plot insert-lookup throughput (separated)
    ax = axs[1]
    offset = 0
    for i, task in enumerate(tasks):
        task_data = []
        for idx in indexs:
            task_data.append(insertlookup_throughput[idx]['lookup'].get(task, 0))
        ax.bar([x + offset for x in index], task_data, bar_width/2, 
               label=f'{task} (lookup)' if offset == 0 else "_nolegend_", 
               color=colors[i])
        offset += bar_width/2
    
    offset = bar_width*2
    for i, task in enumerate(tasks):
        task_data = []
        for idx in indexs:
            task_data.append(insertlookup_throughput[idx]['insert'].get(task, 0))
        ax.bar([x + offset for x in index], task_data, bar_width/2, 
               label=f'{task} (insert)', color=colors[i], hatch='///')
        offset += bar_width/2
    
    ax.set_title('Insert-Lookup Throughput (50% insert ratio)')
    ax.set_ylabel('Throughput (Mops/s)')
    ax.set_xticks([x + bar_width*1.5 for x in index])
    ax.set_xticklabels([insert_labels.get(idx, idx) for idx in indexs], rotation=45, ha='right')
    ax.legend()
    
    # 3. Plot mixed workload with 10% inserts
    ax = axs[2]
    for i, task in enumerate(tasks):
        task_data = []
        for idx in indexs:
            task_data.append(insertlookup_mix1_throughput[idx].get(task, 0))
        ax.bar([x + i*bar_width for x in index], task_data, bar_width, label=task, color=colors[i])
        
    ax.set_title('Mixed Workload (10% insert ratio)')
    ax.set_ylabel('Throughput (Mops/s)')
    ax.set_xticks([x + bar_width*1.5 for x in index])
    ax.set_xticklabels([mix1_labels.get(idx, idx) for idx in indexs], rotation=45, ha='right')
    ax.legend()
    
    # 4. Plot mixed workload with 90% inserts
    ax = axs[3]
    for i, task in enumerate(tasks):
        task_data = []
        for idx in indexs:
            task_data.append(insertlookup_mix2_throughput[idx].get(task, 0))
        ax.bar([x + i*bar_width for x in index], task_data, bar_width, label=task, color=colors[i])
        
    ax.set_title('Mixed Workload (90% insert ratio)')
    ax.set_ylabel('Throughput (Mops/s)')
    ax.set_xticks([x + bar_width*1.5 for x in index])
    ax.set_xticklabels([mix2_labels.get(idx, idx) for idx in indexs], rotation=45, ha='right')
    ax.legend()
    
    fig.suptitle('Benchmark Results - Throughput', fontsize=16)
    plt.tight_layout(rect=[0, 0, 1, 0.95])
    plt.savefig('benchmark_results_throughput.png', dpi=300, bbox_inches='tight')
    plt.show()
    
    # Plot size results
    fig, axs = plt.subplots(2, 2, figsize=(15, 12))
    axs = axs.flatten()
    
    # 1. Plot lookup-only size
    ax = axs[0]
    for i, task in enumerate(tasks):
        task_data = []
        for idx in indexs:
            task_data.append(lookuponly_size[idx].get(task, 0))
        ax.bar([x + i*bar_width for x in index], task_data, bar_width, label=task, color=colors[i])
        
    ax.set_title('Lookup-only Index Size')
    ax.set_ylabel('Size (bytes)')
    ax.set_xticks([x + bar_width*1.5 for x in index])
    ax.set_xticklabels([lookup_labels.get(idx, idx) for idx in indexs], rotation=45, ha='right')
    ax.legend()
    
    # 2. Plot insert-lookup size
    ax = axs[1]
    for i, task in enumerate(tasks):
        task_data = []
        for idx in indexs:
            task_data.append(insertlookup_size[idx].get(task, 0))
        ax.bar([x + i*bar_width for x in index], task_data, bar_width, label=task, color=colors[i])
        
    ax.set_title('Insert-Lookup Index Size (50% insert ratio)')
    ax.set_ylabel('Size (bytes)')
    ax.set_xticks([x + bar_width*1.5 for x in index])
    ax.set_xticklabels([insert_labels.get(idx, idx) for idx in indexs], rotation=45, ha='right')
    ax.legend()
    
    # 3. Plot mixed workload 1 size
    ax = axs[2]
    for i, task in enumerate(tasks):
        task_data = []
        for idx in indexs:
            task_data.append(insertlookup_mix1_size[idx].get(task, 0))
        ax.bar([x + i*bar_width for x in index], task_data, bar_width, label=task, color=colors[i])
        
    ax.set_title('Mixed Workload Index Size (10% insert ratio)')
    ax.set_ylabel('Size (bytes)')
    ax.set_xticks([x + bar_width*1.5 for x in index])
    ax.set_xticklabels([mix1_labels.get(idx, idx) for idx in indexs], rotation=45, ha='right')
    ax.legend()
    
    # 4. Plot mixed workload 2 size
    ax = axs[3]
    for i, task in enumerate(tasks):
        task_data = []
        for idx in indexs:
            task_data.append(insertlookup_mix2_size[idx].get(task, 0))
        ax.bar([x + i*bar_width for x in index], task_data, bar_width, label=task, color=colors[i])
        
    ax.set_title('Mixed Workload Index Size (90% insert ratio)')
    ax.set_ylabel('Size (bytes)')
    ax.set_xticks([x + bar_width*1.5 for x in index])
    ax.set_xticklabels([mix2_labels.get(idx, idx) for idx in indexs], rotation=45, ha='right')
    ax.legend()
    
    fig.suptitle('Benchmark Results - Index Size', fontsize=16)
    plt.tight_layout(rect=[0, 0, 1, 0.95])
    plt.savefig('benchmark_results_size.png', dpi=300, bbox_inches='tight')
    plt.show()
    
    # Save data to CSV files for further analysis
    import os
    os.makedirs('analysis_results', exist_ok=True)
    
    pd.DataFrame(lookuponly_throughput).to_csv('analysis_results/lookuponly_throughput.csv')
    pd.DataFrame(lookuponly_size).to_csv('analysis_results/lookuponly_size.csv')
    
    lookup_df = pd.DataFrame({idx: data['lookup'] for idx, data in insertlookup_throughput.items()})
    insert_df = pd.DataFrame({idx: data['insert'] for idx, data in insertlookup_throughput.items()})
    lookup_df.to_csv('analysis_results/insertlookup_lookup_throughput.csv')
    insert_df.to_csv('analysis_results/insertlookup_insert_throughput.csv')
    pd.DataFrame(insertlookup_size).to_csv('analysis_results/insertlookup_size.csv')
    
    pd.DataFrame(insertlookup_mix1_throughput).to_csv('analysis_results/insertlookup_mix1_throughput.csv')
    pd.DataFrame(insertlookup_mix1_size).to_csv('analysis_results/insertlookup_mix1_size.csv')
    
    pd.DataFrame(insertlookup_mix2_throughput).to_csv('analysis_results/insertlookup_mix2_throughput.csv')
    pd.DataFrame(insertlookup_mix2_size).to_csv('analysis_results/insertlookup_mix2_size.csv')

if __name__ == "__main__":
    result_analysis()
        


        
        
        
        
    