
#ifndef SQL_EXPRESSIONS
#define SQL_EXPRESSIONS

#include "MyDB_AttType.h"
#include "MyDB_Table.h"
#include <string>
#include <vector>
#include <map>
#include <set>

// create a smart pointer for database tables
using namespace std;
class ExprTree;
typedef shared_ptr <ExprTree> ExprTreePtr;

enum ReturnType { stringType, intType, doubleType, boolType, errType };

inline std::string typeToString(ReturnType t) {
	switch (t) {
		case intType: return "int";
		case doubleType: return "double";
		case stringType: return "string";
		case boolType: return "bool";
		case errType: return "error";
	}
	return "unknown";
}

// this class encapsules a parsed SQL expression (such as "this.that > 34.5 AND 4 = 5")

// class ExprTree is a pure virtual class... the various classes that implement it are below
class ExprTree {

public:
	virtual string toString () = 0;
	virtual ReturnType typeCheck (
        map<std::string, MyDB_TablePtr> &allTables,
        vector<std::pair<std::string, std::string>> &tablesToProcess
    ) = 0;
	virtual ~ExprTree () {}

	virtual bool isAggregate() { return false; }

	virtual void getReferencedAttributes(std::set<std::pair<string,string>> &atts) {
		// Default: this expression refers to no attributes.
	}
};

class BoolLiteral : public ExprTree {

private:
	bool myVal;
public:
	
	BoolLiteral (bool fromMe) {
		myVal = fromMe;
	}

	ReturnType typeCheck(map<string, MyDB_TablePtr> &allTables, vector<pair<string, string>> &tablesToProcess) override {
        return boolType;
    }

	string toString () {
		if (myVal) {
			return "bool[true]";
		} else {
			return "bool[false]";
		}
	}	
};

class DoubleLiteral : public ExprTree {

private:
	double myVal;
public:

	DoubleLiteral (double fromMe) {
		myVal = fromMe;
	}

	ReturnType typeCheck(map<string, MyDB_TablePtr> &allTables, vector<pair<string, string>> &tablesToProcess) override {
        return doubleType;
    }

	string toString () {
		return "double[" + to_string (myVal) + "]";
	}	

	~DoubleLiteral () {}
};

// this implement class ExprTree
class IntLiteral : public ExprTree {

private:
	int myVal;
public:

	IntLiteral (int fromMe) {
		myVal = fromMe;
	}

	ReturnType typeCheck(map<string, MyDB_TablePtr> &allTables, vector<pair<string, string>> &tablesToProcess) override {
		return intType;
	}

	string toString () {
		return "int[" + to_string (myVal) + "]";
	}

	~IntLiteral () {}
};

class StringLiteral : public ExprTree {

private:
	string myVal;
public:

	StringLiteral (char *fromMe) {
		fromMe[strlen (fromMe) - 1] = 0;
		myVal = string (fromMe + 1);
	}

	ReturnType typeCheck(map<string, MyDB_TablePtr> &allTables, vector<pair<string, string>> &tablesToProcess) override {
		return stringType;
	}

	string toString () {
		return "string[" + myVal + "]";
	}

	~StringLiteral () {}
};

class Identifier : public ExprTree {

private:
	string tableName;
	string attName;
public:

	Identifier (char *tableNameIn, char *attNameIn) {
		tableName = string (tableNameIn);
		attName = string (attNameIn);
	}

	ReturnType typeCheck(map<string, MyDB_TablePtr> &allTables, vector<pair<string, string>> &tablesToProcess) override {
		// look up the tableName value (an alias) in "tablesToProcess" to get the actual table name
		string actualTableName;

		bool foundAlias = false;
		for (pair<string, string> p : tablesToProcess) {
			if (p.second == tableName) {  // match alias
				actualTableName = p.first;
				foundAlias = true;
				break;
			}
		}

		if (!foundAlias) {
			cout << "ERROR: Table alias '" << tableName << "' not found in query" << endl;
			return errType;
    	}

		// look up the table name in allTables, and use that to look up the type of attName
		auto tableIt = allTables.find(actualTableName);
		if (tableIt == allTables.end()) {
			cout << "ERROR: Table '" << actualTableName << "' not found in catalog" << endl;
			return errType;
		}

		MyDB_TablePtr table = tableIt->second;
		MyDB_SchemaPtr schema = table->getSchema();

		// Look up attribute in schema
		pair<int, MyDB_AttTypePtr> attInfo = schema->getAttByName(attName);
		if (attInfo.first == -1) {
			cout << "ERROR: Attribute '" << attName << "' not found in table '" 
				<< actualTableName << endl;
			return errType;
		}

		MyDB_AttTypePtr attType = attInfo.second;

		// Map MyDB_AttTypePtr to ReturnType
		// Not sure what to return for boolType
		if (attType->isBool()) return boolType;      
		string typeStr = attType->toString();
		if (typeStr == "int") return intType;
		if (typeStr == "double") return doubleType;
		if (typeStr == "string") return stringType;

		return errType;
	}

	string toString () {
		return "[" + tableName + "_" + attName + "]";
	}	

	void getReferencedAttributes(std::set<std::pair<string,string>> &atts) override {
		atts.insert({tableName, attName});
	}

	~Identifier () {}
};

class MinusOp : public ExprTree {

private:

	ExprTreePtr lhs;
	ExprTreePtr rhs;
	
public:

	MinusOp (ExprTreePtr lhsIn, ExprTreePtr rhsIn) {
		lhs = lhsIn;
		rhs = rhsIn;
	}

	ReturnType typeCheck(map<string, MyDB_TablePtr> &allTables, vector<pair<string, string>> &tablesToProcess) override {
		ReturnType leftType = lhs->typeCheck(allTables, tablesToProcess);
		ReturnType rightType = rhs->typeCheck(allTables, tablesToProcess);

		if (leftType == errType || rightType == errType)
			return errType;

		if (leftType == stringType || rightType == stringType) {
			cout << "ERROR: Cannot subtract string values" << endl;
			return errType;
		}

		if (leftType == boolType || rightType == boolType) {
			cout << "ERROR: Cannot subtract bool values" << endl;
			return errType;
		}

		if (leftType == intType && rightType == intType)
			return intType;

		return doubleType;
	}

	bool isAggregate() override {
		return lhs->isAggregate() || rhs->isAggregate();
	}

	void getReferencedAttributes(std::set<std::pair<string,string>> &atts) override {
		lhs->getReferencedAttributes(atts);
		rhs->getReferencedAttributes(atts);
	}

	string toString () {
		return "- (" + lhs->toString () + ", " + rhs->toString () + ")";
	}	

	~MinusOp () {}
};

class PlusOp : public ExprTree {

private:

	ExprTreePtr lhs;
	ExprTreePtr rhs;
	
public:

	PlusOp (ExprTreePtr lhsIn, ExprTreePtr rhsIn) {
		lhs = lhsIn;
		rhs = rhsIn;
	}

	ReturnType typeCheck(map<std::string, MyDB_TablePtr> &allTables, vector<std::pair<std::string, std::string>> &tablesToProcess) {
		ReturnType leftType = lhs->typeCheck (allTables, tablesToProcess);
		ReturnType rightType = rhs->typeCheck (allTables, tablesToProcess);

		// the only way a + returns an error is if the left subexpression or right subexpression returns an error
		if (leftType == errType || rightType == errType)
			return errType;
		
		if (leftType == stringType || rightType == stringType) 
			return stringType;
	
		if (leftType == boolType || rightType == boolType) {
			cout << "ERROR: Cannot add bool values" << endl;
			return errType;
		}

		if (leftType == intType && rightType == intType)
			return intType;

		return doubleType;
	}


	void getReferencedAttributes(std::set<std::pair<string,string>> &atts) override {
		lhs->getReferencedAttributes(atts);
		rhs->getReferencedAttributes(atts);
	}

	bool isAggregate() override {
		return lhs->isAggregate() || rhs->isAggregate();
	}

	string toString () {
		return "+ (" + lhs->toString () + ", " + rhs->toString () + ")";
	}	

	~PlusOp () {}
};

class TimesOp : public ExprTree {

private:

	ExprTreePtr lhs;
	ExprTreePtr rhs;
	
public:

	TimesOp (ExprTreePtr lhsIn, ExprTreePtr rhsIn) {
		lhs = lhsIn;
		rhs = rhsIn;
	}

	ReturnType typeCheck(map<string, MyDB_TablePtr> &allTables, vector<pair<string, string>> &tablesToProcess) override {
		ReturnType leftType = lhs->typeCheck(allTables, tablesToProcess);
		ReturnType rightType = rhs->typeCheck(allTables, tablesToProcess);

		if (leftType == errType || rightType == errType)
			return errType;

		if (leftType == stringType || rightType == stringType) {
			cout << "ERROR: Cannot multiply string values" << endl;
			return errType;
		}

		if (leftType == boolType || rightType == boolType) {
			cout << "ERROR: Cannot multiply bool values" << endl;
			return errType;
		}

		if (leftType == intType && rightType == intType)
			return intType;

		return doubleType;
	}

	bool isAggregate() override {
		return lhs->isAggregate() || rhs->isAggregate();
	}

	void getReferencedAttributes(std::set<std::pair<string,string>> &atts) override {
		lhs->getReferencedAttributes(atts);
		rhs->getReferencedAttributes(atts);
	}

	string toString () {
		return "* (" + lhs->toString () + ", " + rhs->toString () + ")";
	}	

	~TimesOp () {}
};

class DivideOp : public ExprTree {

private:

	ExprTreePtr lhs;
	ExprTreePtr rhs;
	
public:

	DivideOp (ExprTreePtr lhsIn, ExprTreePtr rhsIn) {
		lhs = lhsIn;
		rhs = rhsIn;
	}

	ReturnType typeCheck(map<string, MyDB_TablePtr> &allTables, vector<pair<string, string>> &tablesToProcess) override {
		ReturnType leftType = lhs->typeCheck(allTables, tablesToProcess);
		ReturnType rightType = rhs->typeCheck(allTables, tablesToProcess);

		if (leftType == errType || rightType == errType)
			return errType;

		if (leftType == stringType || rightType == stringType) {
			cout << "ERROR: Cannot divide string values" << endl;
			return errType;
		}

		if (leftType == boolType || rightType == boolType) {
			cout << "ERROR: Cannot divide bool values" << endl;
			return errType;
		}

		return doubleType;
	}

	void getReferencedAttributes(std::set<std::pair<string,string>> &atts) override {
		lhs->getReferencedAttributes(atts);
		rhs->getReferencedAttributes(atts);
	}

	bool isAggregate() override {
		return lhs->isAggregate() || rhs->isAggregate();
	}

	string toString () {
		return "/ (" + lhs->toString () + ", " + rhs->toString () + ")";
	}	

	~DivideOp () {}
};

class GtOp : public ExprTree {

private:

	ExprTreePtr lhs;
	ExprTreePtr rhs;
	
public:

	GtOp (ExprTreePtr lhsIn, ExprTreePtr rhsIn) {
		lhs = lhsIn;
		rhs = rhsIn;
	}

	ReturnType typeCheck(map<string, MyDB_TablePtr> &allTables, vector<pair<string, string>> &tablesToProcess) override {
		ReturnType leftType = lhs->typeCheck(allTables, tablesToProcess);
		ReturnType rightType = rhs->typeCheck(allTables, tablesToProcess);

		// Propagate errors from subexpressions
		if (leftType == errType || rightType == errType)
			return errType;

		// Allow string comparisons only with string
		if (leftType == stringType || rightType == stringType) {
			if (leftType != rightType) {
				cout << "ERROR: Cannot compare incompatible types: "
					<< "left=" << typeToString(leftType) << ", right=" << typeToString(rightType) << endl;
				return errType;
			}

			return boolType;
		}

		// Allow numeric comparisons (int/double)
		if ((leftType == intType || leftType == doubleType) &&
			(rightType == intType || rightType == doubleType)) {
			return boolType;
		}

		// Anything else is invalid (e.g., bool)
		cout << "ERROR: Cannot compare incompatible types: "
			<< "left=" << typeToString(leftType) << ", right=" << typeToString(rightType) << endl;
		return errType;
	}

	string toString () {
		return "> (" + lhs->toString () + ", " + rhs->toString () + ")";
	}	

	~GtOp () {}
};

class LtOp : public ExprTree {

private:

	ExprTreePtr lhs;
	ExprTreePtr rhs;
	
public:

	LtOp (ExprTreePtr lhsIn, ExprTreePtr rhsIn) {
		lhs = lhsIn;
		rhs = rhsIn;
	}

	ReturnType typeCheck(map<string, MyDB_TablePtr> &allTables, vector<pair<string, string>> &tablesToProcess) override {
		ReturnType leftType = lhs->typeCheck(allTables, tablesToProcess);
		ReturnType rightType = rhs->typeCheck(allTables, tablesToProcess);

		// Propagate errors from subexpressions
		if (leftType == errType || rightType == errType)
			return errType;

		// Allow string comparisons only with string
		if (leftType == stringType || rightType == stringType) {
			if (leftType != rightType) {
				cout << "ERROR: Cannot compare incompatible types: "
					<< "left=" << typeToString(leftType) << ", right=" << typeToString(rightType) << endl;
				return errType;
			}

			return boolType;
		}

		// Allow numeric comparisons (int/double)
		if ((leftType == intType || leftType == doubleType) &&
			(rightType == intType || rightType == doubleType)) {
			return boolType;
		}

		// Anything else is invalid (e.g., bool)
		cout << "ERROR: Cannot compare incompatible types: "
			<< "left=" << typeToString(leftType) << ", right=" << typeToString(rightType) << endl;
		return errType;
	}

	string toString () {
		return "< (" + lhs->toString () + ", " + rhs->toString () + ")";
	}	

	~LtOp () {}
};

class NeqOp : public ExprTree {

private:

	ExprTreePtr lhs;
	ExprTreePtr rhs;
	
public:

	NeqOp (ExprTreePtr lhsIn, ExprTreePtr rhsIn) {
		lhs = lhsIn;
		rhs = rhsIn;
	}

	ReturnType typeCheck(map<string, MyDB_TablePtr> &allTables, vector<pair<string, string>> &tablesToProcess) override {
		ReturnType leftType = lhs->typeCheck(allTables, tablesToProcess);
		ReturnType rightType = rhs->typeCheck(allTables, tablesToProcess);

		// Propagate errors from subexpressions
		if (leftType == errType || rightType == errType)
			return errType;

		// Allow string comparisons only with string
		if (leftType == stringType || rightType == stringType) {
			if (leftType != rightType) {
				cout << "ERROR: Cannot compare incompatible types: "
					<< typeToString(leftType) << ", right=" << typeToString(rightType) << endl;
				return errType;
			}

			return boolType;
		}

		// Allow numeric comparisons (int/double)
		if ((leftType == intType || leftType == doubleType) &&
			(rightType == intType || rightType == doubleType)) {
			return boolType;
		}

		// Anything else is invalid (e.g., bool)
		cout << "ERROR: Cannot compare incompatible types: "
			<< "left=" << typeToString(leftType) << ", right=" << typeToString(rightType) << endl;
		return errType;
	}

	string toString () {
		return "!= (" + lhs->toString () + ", " + rhs->toString () + ")";
	}	

	~NeqOp () {}
};

class OrOp : public ExprTree {

private:

	ExprTreePtr lhs;
	ExprTreePtr rhs;
	
public:

	OrOp (ExprTreePtr lhsIn, ExprTreePtr rhsIn) {
		lhs = lhsIn;
		rhs = rhsIn;
	}

	ReturnType typeCheck(map<string, MyDB_TablePtr> &allTables, vector<pair<string, string>> &tablesToProcess) override {
		ReturnType leftType = lhs->typeCheck(allTables, tablesToProcess);
    	ReturnType rightType = rhs->typeCheck(allTables, tablesToProcess);

		if (leftType == errType || rightType == errType)
        	return errType;

		if (leftType != boolType || rightType != boolType) {
        	cout << "ERROR: OR operator requires boolean operands, but got " << typeToString(leftType) << " and " << typeToString(rightType) << "." << endl;
        	return errType;
    	}

		return boolType;
	}

	string toString () {
		return "|| (" + lhs->toString () + ", " + rhs->toString () + ")";
	}	

	~OrOp () {}
};

class EqOp : public ExprTree {

private:

	ExprTreePtr lhs;
	ExprTreePtr rhs;
	
public:

	EqOp (ExprTreePtr lhsIn, ExprTreePtr rhsIn) {
		lhs = lhsIn;
		rhs = rhsIn;
	}

	ReturnType typeCheck(map<string, MyDB_TablePtr> &allTables, vector<pair<string, string>> &tablesToProcess) override {
		ReturnType leftType = lhs->typeCheck(allTables, tablesToProcess);
		ReturnType rightType = rhs->typeCheck(allTables, tablesToProcess);

		// Propagate errors from subexpressions
		if (leftType == errType || rightType == errType)
			return errType;

		// Allow string comparisons only with string
		if (leftType == stringType || rightType == stringType) {
			if (leftType != rightType) {
				cout << "ERROR: Cannot compare incompatible types: "
					<< "left=" << typeToString(leftType) << ", right=" << typeToString(rightType) << endl;
				return errType;
			}

			return boolType;
		}

		// Allow numeric comparisons (int/double)
		if ((leftType == intType || leftType == doubleType) &&
			(rightType == intType || rightType == doubleType)) {
			return boolType;
		}

		// Anything else is invalid (e.g., bool)
		cout << "ERROR: Cannot compare incompatible types: "
			<< "left=" << typeToString(leftType) << ", right=" << typeToString(rightType) << endl;
		return errType;
	}

	string toString () {
		return "== (" + lhs->toString () + ", " + rhs->toString () + ")";
	}	

	~EqOp () {}
};

class NotOp : public ExprTree {

private:

	ExprTreePtr child;
	
public:

	NotOp (ExprTreePtr childIn) {
		child = childIn;
	}

	ReturnType typeCheck(map<string, MyDB_TablePtr> &allTables, vector<pair<string, string>> &tablesToProcess) override {
		ReturnType childType = child->typeCheck(allTables, tablesToProcess);

        if (childType == errType) 
            return errType;

        // Must be boolean
        if (childType != boolType) {
            cout << "ERROR: NOT operator requires a boolean expression, but got type "
                 << typeToString(childType) << endl;
            return errType;
        }

        return boolType;
	}

	string toString () {
		return "!(" + child->toString () + ")";
	}	

	~NotOp () {}
};

class SumOp : public ExprTree {

private:

	ExprTreePtr child;
	
public:

	SumOp (ExprTreePtr childIn) {
		child = childIn;
	}

	ReturnType typeCheck(map<string, MyDB_TablePtr> &allTables, vector<pair<string, string>> &tablesToProcess) override {
		ReturnType childType = child->typeCheck(allTables, tablesToProcess);

		if (childType == errType)
        	return errType;

		// SUM cannot apply to strings or bools
		if (childType == stringType || childType == boolType) {
			cout << "ERROR: Cannot apply SUM to non-numeric attribute: " << child->toString() << endl;
			return errType;
		}

		// Child type should be int or double
		return childType;
	}

	bool isAggregate() override { return true; }

	string toString () {
		return "sum(" + child->toString () + ")";
	}	

	~SumOp () {}
};

class AvgOp : public ExprTree {

private:

	ExprTreePtr child;
	
public:

	AvgOp (ExprTreePtr childIn) {
		child = childIn;
	}

	ReturnType typeCheck(map<string, MyDB_TablePtr> &allTables, vector<pair<string, string>> &tablesToProcess) override {
		ReturnType childType = child->typeCheck(allTables, tablesToProcess);

		if (childType == errType)
        	return errType;

		// AVG cannot apply to strings or bools
		if (childType == stringType || childType == boolType) {
			cout << "ERROR: Cannot apply AVG to non-numeric attribute: " << child->toString() << endl;
			return errType;
		}

		// AVG should always return double
		return doubleType;
	}

	bool isAggregate() override { return true; }

	string toString () {
		return "avg(" + child->toString () + ")";
	}	

	~AvgOp () {}
};

#endif
