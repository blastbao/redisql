#include "stdlib.h"
#include "stdio.h"

#ifndef SYNTAXTREE_H
#define SYNTAXTREE_H

#ifdef __CPLUSPLUS
extern "C" {
#endif

enum sql_node_type{BRANCH=0, KEYWORD, OPERATOR, ID, INT_TYPE_C, FLOAT_TYPE_C, STRING_TYPE_C, BOUND_SYM};

enum sql_action_type {SAT_SELECT=0, SAT_INSERT, SAT_UPDATE, SAT_DELETE, SAT_CREATE_TABLE, SAT_QUIT, SAT_DROP_TABLE, SAT_HELP};

struct stnode
{
	//�������

	int m_nType;

	//�������

	char * m_strName;

	//���ֵ

	union
	{
		int intval;

		char * strval;

		double floatval;
	} m_Val;

	struct stnode * m_pChildList;

	struct stnode * m_pBrotherList;
};

//������

extern struct stnode * syntax_tree_ptr;

//SQL��䶯������

extern enum sql_action_type sql_action;

//������

struct stnode * malloc_node();

//�ͷŽ��

void free_node(struct stnode * p);

//��Ӻ���

int append_child(struct stnode * pParent, struct stnode * pChild);

//��ӡ�����

void print_tree_node(struct stnode * pParent);

//��ӡ��

void print_syntax_tree(struct stnode * pParent);

//�����﷨��

void destroy_syntax_tree(struct stnode * pParent);

//�õ�����

struct stnode * get_child(struct stnode * pParent, int nOrderNo/*��1��ʼ*/);

//�õ���������

int get_child_cnt(struct stnode * pParent);

#ifdef __CPLUSPLUS
}
#endif

#endif //SYNTAXTREE_H


