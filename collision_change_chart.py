import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdatesimport
import csv

changes = []
this_year = []
last_year = []
this_year_inj = []
last_year_inj = []
this_year_kill = []
last_year_kill = []

doys = []
f = open('collision_change_yoy.csv', 'rb')
try:
    reader = csv.DictReader(f)
    for row in reader:
        this_year.append(row['this_year']) 
        last_year.append(row['last_year'])
        this_year_inj.append(row['this_year_inj']) 
        last_year_inj.append(row['last_year_inj'])
        this_year_kill.append(row['this_year_kill']) 
        last_year_kill.append(row['last_year_kill'])
        changes.append(row['yoy_change'])
        doys.append(row['woy'])
finally:
    f.close()



plt.figure(1)
plt.subplot(211)
plt.plot(doys, this_year, 'r', doys, last_year, 'b')

plt.subplot(212)
plt.plot(doys, this_year_inj, 'r', doys, last_year_inj, 'b',)
plt.show()



# plt.plot(doys, this_year, 'r', doys, last_year, 'b' )
#           doys, this_year_inj, 'r--', doys, last_year_inj, 'b--',
#          # doys, this_year_kill, 'r.', doys, last_year_kill, 'b.'
#          )
# plt.show()


# same figure
# fig, ax1 = plt.subplots()
# ax1.plot(doys, this_year, 'green', 
#          doys, last_year, 'lime')
# ax1.set_xlabel('incidents')
# # Make the y-axis label and tick labels match the line color.
# ax1.set_ylabel('cnt', color='k')
# for tl in ax1.get_yticklabels():
#     tl.set_color('k')
# ax2 = ax1.twinx()
# ax2.plot(doys, this_year_inj, 'maroon', 
#          doys, last_year_inj, 'red')
# ax2.set_ylabel('sin', color='k')
# for tl in ax2.get_yticklabels():
#     tl.set_color('k')
# plt.show()