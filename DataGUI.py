from threading import Thread
import pandas as pd
import numpy as np
import time
import graphlab as gl
from Tkinter import *
import tkFileDialog
import os
import sys
from pandasql import sqldf
import re


class GraphLabThread(Thread):
	def __init__(self, graphlab_object):
		super(GraphLabThread, self).__init__()
		self.graphlab_object = graphlab_object
		self.loop = True
		self.started = False
	def run(self):
		while self.loop:
			if self.started:
				time.sleep(2)
			else:
				self.graphlab_object.show()
				self.started = True
				time.sleep(2)
	def kill_thread(self):
		self.loop = False

def addstack(df):
	global stack
	stack.append(df.copy(deep=True))
	print(len(stack))

def loadFile(root):
	global text
	global df
	global dfHead
	global stack
	filepath = tkFileDialog.askopenfilename(parent=root,title='Load CSV', filetypes=[("Comma Seperated File", "*.csv")])
	print os.path.normpath(filepath)
	text.delete('1.0', END)
	if filepath is not ".":
		df = pd.read_csv(filepath)
		text.insert(END, "Successfuly loaded CSV data.")
		displayHead()
	else:
		text.insert(END, "Please try loading the data again.")
	addstack(df)

def sqlQuery(query):
	global df
	global text
	global myThread
	global dfHead
	global stack
	addstack(df)
	clean = lambda x : re.sub(r"[\n\r]", '', x)
	df = sqldf(clean(query), globals())
	text.delete('1.0', END)
	text.insert(END, "Successfuly executed SQL.")
	displayHead()

def displayHead():
	global df
	global text
	global myThread
	global dfHead
	dfHead.delete('1.0', END)
	rows, cols = df.shape
	dfHead.insert(END, "Rows:{} , Columns:{}.\nTable name: df\n".format(rows, cols) + str(df.head()))

def cleanColumnNames():
	global df
	global text
	global myThread
	global dfHead
	global stack
	addstack(df)
	df.columns = list(map(lambda x: x.strip().replace(' ','_'), df.columns))
	df.columns = list(map(lambda x: x.strip().replace('.','_'), df.columns))
	displayHead()

def cleanDataTypes():
	global df
	global text
	global dfHead
	pass

def pivot(index, values, columns, aggFunc):
	global df
	global DEFAULT_VALUES_TEXT
	global DEFAULT_INDEX_TEXT
	global DEFAULT_COLUMNS_TEXT
	global stack
	addstack(df)
	x = eval ("np." + aggFunc)
	clean = lambda x : re.sub(r"[\n\r]", '', x)
	index, values, columns = clean(index), clean(values), clean(columns)
	index = None if index == DEFAULT_INDEX_TEXT else index.split(',')
	values = None if values == DEFAULT_VALUES_TEXT else values.split(',')
	columns = None if columns == DEFAULT_COLUMNS_TEXT else columns.split(',')
	df = pd.pivot_table(df, values=values, index=index, columns= columns, aggfunc=x)
	displayHead()

def launchDashboard():
	global myThread
	global df
	global text
	if myThread is not None:
		try:
			myThread.kill_thread()
			time.sleep(1)
		except:
			text.delete('1.0', END)
			text.insert(END, "Error launching dashboard. Please restart application.")
			return
	if df is not None:
		myThread = GraphLabThread(gl.SFrame(data=df))
		myThread.start()
	else:
		text.delete('1.0', END)
		text.insert(END, "Please load data first.")
def saveCsv(root):
	global text
	global df
	filepath = tkFileDialog.asksaveasfilename(parent=root,title='Load CSV', 
		filetypes=[("Comma Seperated File", "*.csv")], defaultfileextension='.csv')
	print os.path.normpath(filepath)
	text.delete('1.0', END)
	if filepath is not ".":
		df.to_csv(os.path.normpath(filepath))
		text.insert(END, "Successfuly saved CSV data.")
	else:
		text.insert(END, "Please try saving the data again.")


def revert():
	global df
	global stack
	if len(stack) > 1:
		df = stack.pop()
	try:
		displayHead()
	except:
		pass
def exitApp():
	global myThread
	global text
	text.delete('1.0', END)
	text.insert(END, "Exiting...")
	if myThread:
		myThread.kill_thread()
		time.sleep(1)
	sys.exit()

stack = []
myThread = None
df = None
main = Tk()
main.protocol("WM_DELETE_WINDOW", lambda: exitApp())

textAndRevert = Frame(main)
text = Text(textAndRevert, height=2, width=30)
text.pack(side=LEFT)
revertBtn = Button(textAndRevert, text="Undo", command = (lambda: revert()))
revertBtn.pack(side=LEFT)
textAndRevert.pack()

SQLFrame = Frame(main)
console = Text(SQLFrame, height=2, width=30)
console.pack(side=LEFT)
console.insert(END, "Enter SQL query here.")
execSQL = Button(SQLFrame, text="Execute SQL", command= (lambda: sqlQuery(console.get('1.0', END))))
execSQL.pack(side=RIGHT)
SQLFrame.pack()

loadAndGlBtns = Frame(main)
loadBtn = Button(loadAndGlBtns, text="Load CSV", command = (lambda: loadFile(main)))
loadBtn.pack(side=LEFT)
saveBtn = Button(loadAndGlBtns, text="Save CSV", command= (lambda: saveCsv(main)))
saveBtn.pack(side=LEFT)
cleanColumns = Button(loadAndGlBtns, text="Clean Column Names", command = (lambda: cleanColumnNames()))
cleanColumns.pack(side=LEFT)
glBtn = Button(loadAndGlBtns, text="Launch Dashboard", command= (lambda: launchDashboard()))
glBtn.pack(side=LEFT)
loadAndGlBtns.pack()

DEFAULT_INDEX_TEXT = "Col names to group by(comma seperated)"
DEFAULT_COLUMNS_TEXT = "Keys to group by on the pivot table column (comma seperated)"
DEFAULT_VALUES_TEXT = "Col names for values (comma seperated)"

pivotFrame = Frame(main)
index = Text(pivotFrame, height=2, width=28)
index.pack(side=LEFT)
index.insert(END, DEFAULT_INDEX_TEXT)
values = Text(pivotFrame, height=2, width=31)
values.pack(side=LEFT)
values.insert(END, DEFAULT_VALUES_TEXT)
columns = Text(pivotFrame, height=2, width=30)
columns.pack(side=LEFT)
columns.insert(END, DEFAULT_COLUMNS_TEXT)
aggFuncVar = StringVar(pivotFrame)
aggFuncVar.set("sum")
aggFuncSelector = OptionMenu(pivotFrame, aggFuncVar, "sum", "count", "mean", "std")
aggFuncSelector.pack(side=LEFT)
pivotBtn = Button(pivotFrame, text="Pivot", command = (lambda: pivot(index.get('1.0', END), 
	values.get('1.0', END), columns.get('1.0', END), aggFuncVar.get())))
pivotBtn.pack(side=LEFT)
pivotFrame.pack()


dfHead = Text(main, height=30, width=110)
dfHead.pack()

mainloop()
print 'hi'