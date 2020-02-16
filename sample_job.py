import spark
import time
import operator
import pyspark

sample_data = """
This is the Debian GNU/Linux prepackaged version of the Python programming
language. Python was written by Guido van Rossum <guido@cwi.nl> and others.

This package was put together by Klee Dienes <klee@debian.org> from
sources from ftp.python.org:/pub/python, based on the Debianization by
the previous maintainers Bernd S. Brentrup <bsb@uni-muenster.de> and
Bruce Perens.

Current maintainer is Matthias Klose <doko@debian.org> until the final
2.3 version is released.


Copyright notice (as found in LICENSE in the original source).
--------------------------------------------------------------

A. HISTORY OF THE SOFTWARE
==========================

Python was created in the early 1990s by Guido van Rossum at Stichting
Mathematisch Centrum (CWI, see http://www.cwi.nl) in the Netherlands
as a successor of a language called ABC.  Guido remains Python's
principal author, although it includes many contributions from others.

In 1995, Guido continued his work on Python at the Corporation for
National Research Initiatives (CNRI, see http://www.cnri.reston.va.us)
in Reston, Virginia where he released several versions of the
software.

In May 2000, Guido and the Python core development team moved to
BeOpen.com to form the BeOpen PythonLabs team.  In October of the same
year, the PythonLabs team moved to Digital Creations (now Zope
Corporation, see http://www.zope.com).  In 2001, the Python Software
Foundation (PSF, see http://www.python.org/psf/) was formed, a
non-profit organization created specifically to own Python-related
Intellectual Property.  Zope Corporation is a sponsoring member of
the PSF.
""".split('\n')

# conf = pyspark \
#     .SparkConf()\
#     .setAppName('Test Counting App')\
#     .setAll(
#         [
#             ('spark.driver.host', '192.168.0.243'),
#             ('spark.driver.port',6294),
#             ('spark.driver.bindAddress', '0.0.0.0')
#     ]
#     )

conf = pyspark \
    .SparkConf()\
    .setAppName('Test Counting App') \
    .set('spark.driver.cores', '1') \
    .set('spark.executor.cores', '1') \
    .set('spark.driver.memory', '16G') \
    .set('spark.executor.memory', '9G')

conf = pyspark.SparkConf()
conf.set('spark.driver.host','192.168.0.157')

sc = pyspark.SparkContext(master="spark://192.168.0.243:7077", conf=conf)
sc.setLogLevel('WARN')
file = '/home/sysadmin/Projects/spark-jobs/hp7.txt'

# lines = sc.textFile(file)
lines = sc.parallelize(open(file=file, mode='r', encoding="utf-8"), 1000)

words = lines.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1))

counts = words.reduceByKey(operator.add)

sorted_counts = counts.sortBy(lambda x: x[1], False)

# sorted_counts..saveAsTextFile('/home/sysadmin/Projects/spark-jobs/output')
for word, count in sorted_counts.toLocalIterator():
    with open(file='/home/sysadmin/Projects/spark-jobs/output/hp7_word_freq_3.txt', mode='a') as output:
        output.write(f'{word},{count} \n')

"""
harry_count = lines.filter(lambda line: 'harry' in line.lower())

short_lines = lines.filter(lambda line: len(line) < 10)

lines.collect()

harry_count = harry_count.count()
short_lines = short_lines.count()
total_rows = lines.count()

print(f'Found {total_rows} total rows.')
print(f'Found {short_lines} with less than 10 characters')
print(f'Found the name Harry {harry_count} times!')

"""








#
# df = spark.read.format('csv') \
#     .option("inferSchema", True) \
#     .option("header", True) \
#     .load(file)
#
# print(df.columns, df.head())

# txt = sc.textFile('file:////usr/local/spark/data/mllib/avengers.csv')
# txt = sc.textFile('file:////usr/share/doc/python/copyright')
#

# python_lines = sample_data.filter(lambda line: 'python' in line.lower())

    # .filter(lambda line: 'stark' in line.lower()) \
    # .collect()

# print(f'Found {sample_data.count()} total lines.')
# print(f'Found {python_lines.count()} lines with the keywork python.')

# tony_start_lines = txt.filter(lambda line: 'stark' in line.lower())
# print(tony_start_lines.count())