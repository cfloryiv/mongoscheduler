from datetime import datetime, timedelta
from mongoengine import connect, Document, EmbeddedDocument, fields
import csv
import os
from pathlib import Path


class PickupDetail(EmbeddedDocument):
    pickupdate = fields.DateField()
    pickupproduct = fields.StringField()
    pickupamount = fields.FloatField()

    def __str__(self):
        return f"<Pickupdetail | date: {self.pickupdate}, product: {self.pickupproduct}, amount: {self.pickupamount}>"


class PickupClient(Document):
    name = fields.StringField()
    state_city = fields.StringField()
    street = fields.StringField()
    address = fields.StringField()
    pickupdetail = fields.EmbeddedDocumentListField(PickupDetail)

    # earliestdate = fields.DateField()
    # latestdate = fields.DateField()

    def __str__(self):
        return f"<PickupClient | name: {self.name}, address: {self.address}>"


def clear_data():
    PickupClient.drop_collection()
    print("Cleared Data!")


def importInputFile(filename):
    print('importing ', filename)
    count = 0
    empty = 0
    with open(f'C:\\users\\cflor\\BB\\inputFiles\\{filename}') as file:

        reader = csv.reader(file)
        for row in reader:

            count += 1
            if count == 1:
                empty += 1
                continue
            if row[0] == '':
                empty += 1
                continue
            # print(row)

            # create pickup record
            pickupdate = datetime.strptime(row[0], '%m/%d/%Y')
            name = row[1]
            address = row[2]
            pickupproduct = row[3]
            try:
                pickupamount = row[4]
            except:
                pickupamount = 0

            ax = address.split(',')
            state = ax[len(ax) - 1]
            street = ax[0]
            if len(ax) == 2:
                ay = ax[0].split(' ')
                city = ay[len(ay) - 1]
            else:
                city = ax[1]
            state_city = state + ', ' + city
            if street=='unknown':
                address=city+', '+state
            # build detail record
            pickupdetailx = PickupDetail(pickupdate=pickupdate, pickupproduct=pickupproduct, pickupamount=pickupamount)

            # read in document for client
            pickupclient = PickupClient.objects(name=name, state_city=state_city).first()
            if pickupclient == None:
                pickupclient = PickupClient(name=name, state_city=state_city, address=address)
            pickupclient.pickupdetail.append(pickupdetailx)
            pickupclient.save()

            # print(pickupclient)

    return


def importFiles():
    dirpath = 'C:\\users\\cflor\\BB\\inputFiles'
    paths = sorted(Path(dirpath).iterdir(), key=os.path.getmtime)

    # for file in os.scandir('C:\\users\\cflor\\BB\\inputFiles'):
    for file in paths:
        importInputFile(file.name)


def createExportFile():
    filename = 'pickup.csv'
    splitDate=datetime.now()-timedelta(days=30)

    with open(f'C:\\users\\cflor\\BB\\outputFiles\\{filename}', 'w', newline='') as csvfile:
        fieldnames = ['date', 'name', 'address', 'sales', 'days', 'p1', 'p2', 'p3', 'p4', 'p5']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()



        pickupclients = PickupClient.objects({})

    # sort predict file by pickup region

        predict_date = 0
        predict_days = 0
        # get last 6 delivery dates

        puDays=[]



        for pickupclient in pickupclients:
            puDays=[]
            numberPickups=0
            sales=0
            for pud in pickupclient.pickupdetail:
                d=pud.pickupdate
                puDays.append(d)
                if numberPickups==0:
                    lowerDate=d
                    upperDate=d
                if d<lowerDate:
                    lowerDate=d
                if d>upperDate:
                    upperDate=d
                numberPickups+=1
                sales+=pud.pickupamount
                for d in range(5):
                    puDays.append(lowerDate)
            puDays.sort()
            days=int((upperDate-lowerDate).days/numberPickups)
            if days<14:
                days=14
            predict_days=days
            predict_date=upperDate+timedelta(days=predict_days)
            lower= len(puDays)-6
            pickupDays=[]
            for d in range(7):
                pickupDays.append(puDays[lower+d-1])
            writer.writerow({'date': predict_date, 'name': pickupclient.name,
                         'address': pickupclient.address, 'sales': sales,
                         'days': predict_days,
                         'p1': pickupDays[6],
                         'p2': pickupDays[5],
                         'p3': pickupDays[4],
                         'p4': pickupDays[3],
                         'p5': pickupDays[2]
                         })


        writer.writerow({'date': splitDate.strftime('%m/%d/%Y'), 'name': '=========',
                 'address': '===============', 'sales': '=',
                 'days': '=',
                 'p1': '=========',
                 'p2': '=========',
                 'p3': '=========',
                 'p4': '=========',
                 'p5': '========='
                 })
def addData():
    filename=input('Enter file name (ex: cbf-pickup-May 1, 2023.csv): ')
    with open(f'C:\\users\\cflor\\BB\\inputFiles\\{filename}', 'w', newline='') as csvfile:
        fieldnames = ['date', 'name', 'address', 'product', 'amount']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        while True:
            puDate=input('enter pickup date or end: ')
            if puDate=='end':
                break
            while True:
                name=input('enter client name or new: ')
                pickupclients=PickupClient.objects( name=name)
                addresses=[]
                ctr=0
                for pickupclient in pickupclients:
                    addresses.append(pickupclient.address)
                    ctr+=1
                    print(f'{ctr}={pickupclient.address}')
                    if ctr==0:
                        print('cannot locate name, try another')
                ans=input('enter address id: ')
                if ans=='':
                    ans=0
                else:
                    ans=int(ans)
                if ans>=1 and ans<=ctr:
                    address=addresses[ctr-1]
                    break
                else:
                    address=input('enter pickup address or enter to try again: ')
                    if address=='':
                        continue
            amount=input('enter amount: ')

            writer.writerow({'date': puDate,
                    'name': name,
                    'address': address,
                    'product': 'strips',
                    'amount': amount
                             })


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    connect("pickups_me")
    print("connected")
    ans=input('(a)dd new data? ')
    if ans=='a':
        addData()
    else:
        clear_data()
        importFiles()
        createExportFile()
