import csv
from collections import defaultdict

def load_lookup_table(lookup_file):
    lookup_table = {}
    with open(lookup_file, mode='r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            dstport = int(row['dstport'].strip())
            protocol = row['protocol'].strip().lower()  
            tag = row['tag'].strip()
            lookup_table[(dstport, protocol)] = tag
    return lookup_table

def parse_flow_logs(flow_file, lookup_table):
    tag_counts = defaultdict(int)
    port_protocol_counts = defaultdict(int)
    untagged_count = 0

    with open(flow_file, mode='r') as file:
        for line in file:
            parts = line.split()
            dstport = int(parts[5])  # Destination port
            protocol_number = parts[7]  # Protocol number

            protocol = None
            if protocol_number == '6':
                protocol = 'tcp'
            elif protocol_number == '17':
                protocol = 'udp'
            elif protocol_number == '1':
                protocol = 'icmp'
            key = (dstport, protocol)
            if key in lookup_table:
                tag = lookup_table[key]
                tag_counts[tag] += 1
            else:
                untagged_count += 1
           

            port_protocol_counts[key] += 1

    return tag_counts, port_protocol_counts, untagged_count

# Function to write the results to an output file
def write_output(output_file, tag_counts, port_protocol_counts, untagged_count):
    with open(output_file, mode='w') as file:
        file.write("Tag Counts:\n")
        file.write("Tag,Count\n")
        for tag, count in tag_counts.items():
            file.write(f"{tag},{count}\n")
        file.write(f"Untagged,{untagged_count}\n")


        file.write("\nPort/Protocol Combination Counts:\n")
        file.write("Port,Protocol,Count\n")
        for (dstport, protocol), count in port_protocol_counts.items():
            file.write(f"{dstport},{protocol},{count}\n")

# Main function to run the program
def main():
    lookup_file = 'lookup_table.csv'  # Path to lookup table
    flow_file = 'flow_log.txt'        # Path to flow log file
    output_file = 'output.txt'        # Path to output file

    lookup_table = load_lookup_table(lookup_file)
    tag_counts, port_protocol_counts, untagged_count = parse_flow_logs(flow_file, lookup_table)
    write_output(output_file, tag_counts, port_protocol_counts, untagged_count)

if __name__ == "__main__":
    main()
