from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("DegreesOfSeparation")
sc = SparkContext(conf=conf)

startCharacterID = 5306 # Spiderman
targetCharacterID = 14 # ADAM

# Our accumulator, used to signal when we find the target character during our BFS traversal
hitCounter = sc.accumulator(0)

def convertBFS(line):
    fields = line.split()
    heroID = int(fields[0])
    connections = []
    for connection in fields[1:]:
        connections.append(int(connection))

    color = "WHITE"
    distance = 9999

    if heroID == startCharacterID:
        color = "GRAY"
        distance = 0

    return (heroID, (connections, distance, color))

def createStartingRdd():
    inputFile = sc.textFile("marvel+Graph")
    return inputFile.map(convertBFS)

def bfsMap(node):
    characterID = node[0]
    data = node[1]
    connections = data[0]
    distance = data[1]
    color = data[2]

    results = []

    # If this node needs to be expanded
    if color == "GRAY":
        for connection in connections:
            newCharacterID = connection
            newDistance = distance + 1
            newColor = 'GRAY'
            if targetCharacterID == connection:
                hitCounter.add(1)

            newEntry = (newCharacterID, ([], newDistance, newColor))
            results.append(newEntry)
        
        # We've processed this node, so color it black
        color = 'BLACK'

    # Emit the input node so we don't lose it
    results.append((characterID, (connections, distance, color)))
    return results

def bfsReduce(data1, data2):
    edges1 = data1[0]
    edges2 = data2[0]
    distance1 = data1[1]
    distance2 = data2[1]
    color1 = data1[2]
    color2 = data2[2]

    distance = 9999
    color = 'WHITE'
    edges = []

    # See if one is the original node with its connections
    # If so preserce them
    if len(edges1) > 0 :
        edges = edges1
    elif len(edges) > 0:
        edges = edges2

    # Preserve minimum distance
    if distance1 < distance2:
        distance = distance1

    # Preserve darkest color
    if color1 == 'WHITE' and (color2 == "GRAY" or color2 == "BLACK"):
        color = color2
    if (color1 == 'GRAY' and color2 == 'BLACK'):
        color = color2
    
    return (edges, distance, color)

# Main program
iterationRdd = createStartingRdd()

for iteration in range(0, 10):
    print("Running BFS iteration# " + str(iteration + 1))

    mapped = iterationRdd.flatMap(bfsMap)

    print("Processing " + str(mapped.count()) + " values.")

    if hitCounter.value > 0:
        print("Hit the target character. From " + str(hitCounter.value) + " different direction(s).")
        break

    iterationRdd = mapped.reduceByKey(bfsReduce)