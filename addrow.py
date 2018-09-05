import csv   
fields=['2018-01-01','10.0','50.0', '10.0']
with open(r'export.csv', 'a') as f:
    writer = csv.writer(f)
    writer.writerow(fields)