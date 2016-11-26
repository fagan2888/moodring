"""
A hack for an IBM Article I wrote:

1.  Works with gmail.
2.  transforms it into parts of speech.
3.  Creates 3 word phrases: VERB "to" VERB
4.  sorts by frequency and displays as Google chart.



"""

import imaplib
import email

from collections import defaultdict
from operator import itemgetter
from multiprocessing import Pool

import nltk

username = "username@gmail.com"
password = "yourpassword"
server = "imap.gmail.com"
folder = "INBOX"

page_template = """
<html>
  <head>
    <!--Load the AJAX API-->
    <script type="text/javascript" src="https://www.google.com/jsapi"></script>
    <script type="text/javascript">

      // Load the Visualization API and the piechart package.
      google.load('visualization', '1', {'packages':['corechart']});

      // Set a callback to run when the Google Visualization API is loaded.
      google.setOnLoadCallback(drawChart);

      // Callback that creates and populates a data table,
      // instantiates the pie chart, passes in the data and
      // draws it.
      function drawChart() {

      // Create our data table.
        var data = new google.visualization.DataTable();
        data.addColumn('string', '3 Word "To" Phrase');
        data.addColumn('number', 'Occurances in Inbox');
        data.addRows(%s

        );

        // Instantiate and draw our chart, passing in some options.
        var chart = new google.visualization.PieChart(document.getElementById('chart_div'));
        chart.draw(data, {width: 1200, height: 1200, is3D: true, title: 'Customer Service Email Phrases'});
      }
    </script>
  </head>

  <body>
    <!--Div that will hold the pie chart-->
    <div id="chart_div"></div>
  </body>
</html>


"""


def connect_inbox():
    mail=imaplib.IMAP4_SSL(server, 993)
    mail.login(username,password)
    mail.select(folder)
    status, count = mail.search(None, 'ALL')
    try:
        for num in count[0].split():
            status, data = mail.fetch(num, '(RFC822)')
            yield email.message_from_string(data[0][1])
    finally:
        mail.close()
        mail.logout()

def get_plaintext(messages):
    """Retrieve text/plain version of message"""
    for message in messages:
        for part in message.walk():
            if part.get_content_type() == "text/plain":
                yield part

def transform(messages):
    """Transforms data by tokensizing and tagging parts of speech"""
    for message in messages:
        sentences = nltk.sent_tokenize(str(message))
        sentences = [nltk.word_tokenize(sent) for sent in sentences]
        sentences = [nltk.pos_tag(sent) for sent in sentences]
        yield sentences

def three_letter_phrase(messages):
    """Yields a three word phrase with TO"""
    for message in messages:
        for sentence in message:
            for (w1,t1), (w2,t2), (w3,t3) in nltk.trigrams(sentence):
                if (t1.startswith('V') and t2 == 'TO' and t3.startswith('V')):
                    yield ((w1,w2,w3), 1)

def mapper():
    messages = connect_inbox()
    text_messages = get_plaintext(messages)
    transformed = transform(text_messages)
    for item,count in three_letter_phrase(transformed):
        yield item, count

def phrase_partition(phrases):
    partitioned_data = defaultdict(list)
    for phrase, count in phrases:
        partitioned_data[phrase].append(count)
    return partitioned_data.items()

def reducer(phrase_key_val):
    phrase, count = phrase_key_val
    return [phrase, sum(count)]

def start_mr(mapper_func, reducer_func, processes=1):
    pool = Pool(processes)
    map_output = mapper_func()
    partitioned_data = phrase_partition(map_output)
    reduced_output = pool.map(reducer_func, partitioned_data)
    return reduced_output

def print_report(sort_list, num=15):
    results = []
    for items in sort_list[0:num]:
        phrase = " ".join(items[0])
        result = [phrase, items[1]]
        results.append(result)
    page = page_template % results
    print page

def main():
    phrase_stats = start_mr(mapper, reducer)
    sorted_phrase_stats = sorted(phrase_stats, key=itemgetter(1), reverse=True)
    print_report(sorted_phrase_stats)

if __name__ == "__main__":
    main()
