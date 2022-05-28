import pandas as pd
import requests, xlsxwriter, pprint, datetime
pd.options.mode.chained_assignment = None  # default='warn'

# GroupMe API auth data
TOKEN = ''
GROUPID = ''
BASEURL = 'https://api.groupme.com/v3'
groupEndPoint = '/groups/%s?' % (GROUPID)
messagesEndPoint = '/groups/%s/messages?' % (GROUPID)
LIMIT = 100
BEFORE_ID = ''

# main message data request function
def getRequest(endpoint, token, limit, beforeID):
    tokenParam = 'token=%s' % (token)
    limitParam = 'limit=%s' % (limit)
    beforeIDParam = 'before_id=%s' % (beforeID)
    url = BASEURL + endpoint + tokenParam + '&' + limitParam
    if beforeID != '':
        url += '&' + beforeIDParam
    request = requests.get(url)
    print(url)
    return request

# grab group info function
def getGroup(endpoint, token):
    tokenParam = 'token=%s' % (token)
    url = BASEURL + endpoint + tokenParam
    request = requests.get(url)
    return request

# create members ID-names dictionary
groupData = getGroup(groupEndPoint, TOKEN)
groupJson = groupData.json()
memberList = groupJson['response']['members']
memberIDs = {}

for member in memberList:
    userID = member['user_id']
    memberIDs[userID] = member['name']

# overwrite Ed nickname to Ed Michaelson
memberIDs['3269722'] = 'Ed Michaelson'

# grab most recent set of 100 messages
dataset = []
request = getRequest(messagesEndPoint, TOKEN, LIMIT, BEFORE_ID)
jsonData = request.json()
messageList = jsonData['response']['messages']
totalNumMessages = jsonData['response']['count']
messagesLength = len(messageList)
dataset.extend(messageList)

# while loop to paginate data
while messagesLength == LIMIT:
    # if len(dataset) >= 500:
    #     break
    BEFORE_ID = dataset[-1]['id']
    request = getRequest(messagesEndPoint, TOKEN, LIMIT, BEFORE_ID)
    jsonData = request.json()
    messageList = jsonData['response']['messages']
    messagesLength = len(messageList)
    dataset.extend(messageList)
    status = float(len(dataset) / totalNumMessages) * 100
    print('%.4f' % status + '% completed')

# pprint.pprint(dataset)
# pprint.pprint(memberIDs)

# convert json data & memberIDs to dfs
df = pd.DataFrame(dataset)
df = df.drop(columns='avatar_url')
userDf = pd.DataFrame(memberIDs.values())

# add number of likes per message to df column
def countLikes(row):
    numLikes = len(row['favorited_by'])
    return numLikes

df['numLikes'] = df.apply(lambda row: countLikes(row), axis=1)

# add name of user to each message
def userIDtoName(userID):
    try:
        return memberIDs[userID]
    except:
        memberIDs[userID] = userID
        return memberIDs[userID]

df['person'] = df['user_id'].apply(userIDtoName)

# convert timestamp to date and add date and year columns
def getDate(timestamp):
    return datetime.datetime.fromtimestamp(timestamp)
def getYear(timestamp):
    return datetime.datetime.fromtimestamp(timestamp).year
def getMonth(timestamp):
    return datetime.datetime.fromtimestamp(timestamp).month

df['date'] = df['created_at'].apply(getDate)
df['year'] = df['created_at'].apply(getYear)
df['month'] = df['created_at'].apply(getMonth)

# tag each row as human or not
def isHuman(row):
    if row['person'] == 'system' or row['person'] == 'calendar':
        return 'no'
    else:
        return 'yes'

df['isHuman'] = df.apply(lambda row: isHuman(row), axis=1)

# list of all nicknames per member - df with dropdups of 'name' and 'person'
nicknameDf = df[['person', 'name']]
nicknameDf = nicknameDf.drop_duplicates(['person', 'name'])

# create df with users as columns and messages as rows, with bool if user liked message
likeDf = df[['id', 'favorited_by']]

def countLikesGiven(row, id):
    likeList = row['favorited_by']
    return id in likeList

for k in memberIDs.keys():
    name = memberIDs[k]
    likeDf[name] = likeDf.apply(lambda row: countLikesGiven(row, k), axis=1)

writer = pd.ExcelWriter('GroupMeData2021.xlsx', engine='xlsxwriter')
df.to_excel(writer, sheet_name='data', index=False)
userDf.to_excel(writer, sheet_name='users', index=False)
nicknameDf.to_excel(writer, sheet_name='nicknames', index=False)
likeDf.to_excel(writer, sheet_name='likesGiven', index=False)
writer.save()
