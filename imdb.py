import sys, json, httplib, urllib, socket


if (len(sys.argv) != 3):
        sys.exit("Usage: %s <input filename> <output filename>" % sys.argv[0])


in_file = open(sys.argv[1], 'r')
out_file = open(sys.argv[2], 'a')

for line in in_file:
        tokens = line.strip().split(",")
        title = tokens[2]

        print "processing %s" % tokens[0]

        connection = httplib.HTTPConnection("www.imdbapi.com")
        query = "/?t=%s" % urllib.quote_plus(title)
        try:
                connection.request("GET", query)
        except socket.gaierror:
                print "Retrying to request..."

        response = connection.getresponse()

        try:
                json_data = json.loads(response.read())

                if 'Released' in json_data:
                        released = json_data['Released']
                else:
                        released = tokens[1]

        except ValueError:
                released = 'N/A'

        out_file.write("%s\n" % ",".join([tokens[0], released, tokens[2]]))

connection.close()
