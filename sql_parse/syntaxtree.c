#include "syntaxtree.h"
#include "stdlib.h"
#include "stdio.h"
#include "string.h"


//������

struct stnode * syntax_tree_ptr;

//SQL��䶯������

enum sql_action_type sql_action;

//������

struct stnode * malloc_node()
{
	struct stnode * p = (struct stnode *)malloc(sizeof(struct stnode));

	if(!p)
	{
		return NULL;
	}

	p->m_nType = 0;

	p->m_strName = NULL;

	p->m_Val.intval = 0;

	p->m_Val.strval = NULL;

	p->m_Val.floatval = 0;

	p->m_pChildList = 0;

	p->m_pBrotherList = 0;

	return p;
}

//�ͷŽ��

void free_node(struct stnode * p)
{
	if(p)
	{
		free(p);
	}
}

//��Ӻ���

int append_child(struct stnode * pParent, struct stnode * pChild)
{
	if(!pChild)
	{
		printf("append failed\n");

		return 0;
	}

	if(!pParent)
	{
		printf("pParent==NULL\n");
		
		return 0;
	}

	struct stnode * pTemp = pParent->m_pChildList;

	if(!pTemp)
	{
		pParent->m_pChildList = pChild;

		return 1;
	}

	while(pTemp->m_pBrotherList)
	{
		pTemp = pTemp->m_pBrotherList;		
	}

	pTemp->m_pBrotherList = pChild;

	return 1;
}

//��ӡ�����

void print_tree_node(struct stnode * pParent)
{
	if(!pParent)
	{
		return ;
	}

	if(pParent->m_nType == BRANCH)
	{
		printf("%s", pParent->m_strName);
	}
	else if(pParent->m_nType == KEYWORD)
	{
		printf("%s", pParent->m_strName);
	}
	else if(pParent->m_nType == OPERATOR)
	{
		printf("%s", pParent->m_strName);
	}
	else if(pParent->m_nType == ID)
	{
		printf("%s", pParent->m_strName);
	}
	else if(pParent->m_nType == INT_TYPE_C)
	{
		printf("%d", pParent->m_Val.intval);
	}
	else if(pParent->m_nType == FLOAT_TYPE_C)
	{
		printf("%f", pParent->m_Val.floatval);
	}
	else if(pParent->m_nType == STRING_TYPE_C)
	{
		printf("%s", pParent->m_Val.strval);
	}
	else if(pParent->m_nType == BOUND_SYM)
	{
		printf("%s", pParent->m_strName);		
	}

	return ;
}


//�ڲ�-��ӡ��

void print_tree(struct stnode * pParent, int nLayer)
{
	struct stnode * pList = NULL;

	if(!pParent)
	{
		return ;
	}

	//��ӡ

	int i = 0;

	for(i = 0; i < nLayer - 1; i++)
	{
		printf(" ");
	}

	printf("|-");

	print_tree_node(pParent);

	printf("\n");	

	//��ӡ����

	pList = pParent->m_pChildList;

	while(pList)
	{
		print_tree(pList, nLayer + 1);
		
		pList = pList->m_pBrotherList;
	}
}

//��ӡ��

void print_syntax_tree(struct stnode * pParent)
{
	print_tree(pParent, 1);
}

//�ڲ�ʹ��

void destroy_tree(struct stnode * pParent)
{
	struct stnode * pList = NULL;

	struct stnode * pTemp = NULL;

	if(!pParent)
	{
		return ;
	}

	//��ӡ����

	pList = pParent->m_pChildList;

	while(pList)
	{
		pTemp = pList->m_pBrotherList;

		destroy_tree(pList);

		pList = pTemp;	
	}

	pParent->m_pChildList = NULL;

	//������ַ������ͷ��ַ���

	if(pParent->m_nType == STRING_TYPE_C)
	{
		if(pParent->m_Val.strval)
		{
			free(pParent->m_Val.strval);

			pParent->m_Val.strval = NULL;
		}
	}

	free(pParent);

	return ;	
}

//�����﷨��

void destroy_syntax_tree(struct stnode * pParent)
{
	destroy_tree(pParent);
}

//�õ�����

struct stnode * get_child(struct stnode * pParent, int nOrderNo/*��1��ʼ*/)
{
	if(!pParent)
	{
		return NULL;
	}

	struct stnode * pChild = pParent->m_pChildList;

	int n = 0;

	while(pChild)
	{
		n++;

		if(n == nOrderNo)
		{
			return pChild;						
		}
		
		pChild = pChild->m_pBrotherList;

	}

	return NULL;
}

//�õ���������

int get_child_cnt(struct stnode * pParent)
{
	if(!pParent)
	{
		return -1;
	}

	struct stnode * pChild = pParent->m_pChildList;

	int n = 0;

	while(pChild)
	{
		n++;		
		
		pChild = pChild->m_pBrotherList;

	}

	return n;	
}


