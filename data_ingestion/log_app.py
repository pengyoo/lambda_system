import time
import random
import config

def produce_file_logs(filename):
    
    while True:
        # Generate random
        timestamp = int(time.time())
        datetime = time.strftime("%Y-%m-%d-%H.%M.%S", time.localtime(time.time() - random.randint(0, 100000)))
        date = datetime[0:10]
        node = f"R{random.randint(0, 99):02}-M{random.randint(0, 99):02}-N{random.randint(0, 99):02}-C:J{random.randint(0, 99):02}-U{random.randint(0, 99):02}"
        log_level = random.choice(['INFO', 'WARNING', 'ERROR', 'FATAL'])
        message_content = random.choice(['major internal error', 'minor error', 'network issue', 'disk full', 'Lustre mount FAILED', 're-synch state'])

        log_entry = f"- {timestamp} {date} {node} {datetime}.{str(random.randint(100000, 999999))} {node} RAS KERNEL {log_level} {message_content}"

        print(log_entry)
        with open(filename, 'a') as f:
            f.write(log_entry + '\n')
            
        time.sleep(3)
        # print(log_entry)

if __name__ == "__main__":
    produce_file_logs("/home/hadoop/logs/app.log")