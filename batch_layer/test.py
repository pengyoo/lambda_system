line = "- 1117838571 2005.11.03 R02-M1-N0-C:J12-U11 2005-06-03-15.42.51.749199 R02-M1-N0-C:J12-U11 RAS KERNEL INFO instruction cache parity error corrected"
columns = line.split(" ")
month = int(columns[2][5:7])
level = columns[8]
timestamp = columns[4]
message = ' '.join(columns[9:])

print(month, level, message)
if (month in [10, 11]) and (level == 'INFO') and ('instruction' in message):
    print(timestamp, 1)
    

columns = line.split(" ")
year = int(columns[2][:4])
month = int(columns[2][5:7])
message = ' '.join(columns[9:])
print(month, year)