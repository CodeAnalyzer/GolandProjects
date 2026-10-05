package sql

import (
	"strings"
	"testing"
	"unicode/utf8"
)

func TestParseContent_ProcedureDescription_TypicalHeaderAfterAs(t *testing.T) {
	parser := NewParser()
	content := `#include <macros.h>

DCL_PROC_BEGIN(ReturnCashFund_Insert)
                 @ContractID    DSIDENTIFIER,
                 @Amount        DSMONEY
as
/*----------------------------------------------------------------------
ReturnCashFund_Insert.sql

ReturnCashFund_Insert - возврат сумм из Банка-партнера на счет клиента

@ContractID    - идентификатор договора
@Amount        - сумма возврата

Возвращает 0 или код ошибки.
----------------------------------------------------------------------*/
__BEGIN_PROCEDURE__(ReturnCashFund_Insert)
  declare @RetVal int
`
	result, err := parser.ParseContent(content)
	if err != nil {
		t.Fatalf("ParseContent: %v", err)
	}
	if len(result.Procedures) != 1 {
		t.Fatalf("procedures = %d, want 1", len(result.Procedures))
	}
	proc := result.Procedures[0]
	want := "ReturnCashFund_Insert - возврат сумм из Банка-партнера на счет клиента\n\n@ContractID    - идентификатор договора\n@Amount        - сумма возврата\n\nВозвращает 0 или код ошибки."
	if proc.Description != want {
		t.Fatalf("description mismatch:\ngot:  %q\nwant: %q", proc.Description, want)
	}
}

func TestParseContent_ProcedureDescription_CreateProcWithoutBeginMarker(t *testing.T) {
	parser := NewParser()
	content := `if object_id('ChangeCalendar_CopyPrc') is not null
  drop proc ChangeCalendar_CopyPrc
go

create proc ChangeCalendar_CopyPrc
as
/* ----------------------------------------------------------------------
ChangeCalendar.SQL

ChangeCalendar_CopyPrc - копирование % ставки с БП для договоров
---------------------------------------------------------------------- */
PROFILE_BEGIN_EX('ChangeCalendar_CopyPrc started...')
  declare @RetVal int
`
	result, err := parser.ParseContent(content)
	if err != nil {
		t.Fatalf("ParseContent: %v", err)
	}
	if len(result.Procedures) != 1 {
		t.Fatalf("procedures = %d, want 1", len(result.Procedures))
	}
	got := result.Procedures[0].Description
	if !strings.Contains(got, "копирование % ставки с БП") {
		t.Fatalf("description = %q, want contains ставку-описание", got)
	}
	if strings.Contains(got, "ChangeCalendar.SQL") {
		t.Fatalf("description = %q, имя файла должно быть вырезано", got)
	}
}

func TestParseContent_ProcedureDescription_APICreateProcStyle(t *testing.T) {
	parser := NewParser()
	content := `#include <macros.h>

#define M_HELPER 1

API_CREATE_PROC(API_CCred_BindClassifier)
/*------------------------------------------------------------------------------------------
api_ContractCredit.sql

Процедура API_CCred_BindClassifier - Метод осуществляет привязку кредитных договоров к классификатору.

@ClassifierID - Идентификатор классификатора
------------------------------------------------------------------------------------------*/

  __BEGIN_PROCEDURE__(API_CCred_BindClassifier)
  declare @RetVal int
`
	result, err := parser.ParseContent(content)
	if err != nil {
		t.Fatalf("ParseContent: %v", err)
	}
	if len(result.Procedures) != 1 {
		t.Fatalf("procedures = %d, want 1", len(result.Procedures))
	}
	got := result.Procedures[0].Description
	if !strings.Contains(got, "привязку кредитных договоров к классификатору") {
		t.Fatalf("description = %q, want описание API-процедуры", got)
	}
	if strings.Contains(got, "api_ContractCredit.sql") {
		t.Fatalf("description = %q, имя файла должно быть вырезано", got)
	}
}

func TestParseContent_ProcedureDescription_Absent(t *testing.T) {
	parser := NewParser()
	content := `DCL_PROC_BEGIN(NoDesc_Proc)
  @ID DSIDENTIFIER
as
begin
  select 1
end`
	result, err := parser.ParseContent(content)
	if err != nil {
		t.Fatalf("ParseContent: %v", err)
	}
	if len(result.Procedures) != 1 {
		t.Fatalf("procedures = %d, want 1", len(result.Procedures))
	}
	if result.Procedures[0].Description != "" {
		t.Fatalf("description = %q, want пустая строка", result.Procedures[0].Description)
	}
}

func TestParseContent_ProcedureDescription_BeginBeforeBeginMarker(t *testing.T) {
	// Стиль AcqCalc_ChildThreadState_End: as → /*desc*/ → begin → __BEGIN_PROCEDURE__
	parser := NewParser()
	content := `DCL_PROC_BEGIN(AcqCalc_ChildThreadState_End)
                   @CountSuccess DSINT_KEY = NULL output
as
/*
AcqCalc_ChildThreadState_End - Завершение дочернего потока
*/
begin
  __BEGIN_PROCEDURE__(AcqCalc_ChildThreadState_End)
  declare @ParentSPID DSSPID
`
	result, err := parser.ParseContent(content)
	if err != nil {
		t.Fatalf("ParseContent: %v", err)
	}
	if len(result.Procedures) != 1 {
		t.Fatalf("procedures = %d, want 1", len(result.Procedures))
	}
	got := result.Procedures[0].Description
	if got != "AcqCalc_ChildThreadState_End - Завершение дочернего потока" {
		t.Fatalf("description = %q", got)
	}
}

func TestParseContent_ProcedureDescription_DecorativeLinesStripped(t *testing.T) {
	parser := NewParser()
	content := `create proc Cons_ExtAttrSelect
as
/* ------------------------------------------------------------------------------------------
EffectiveRate.sql

Cons_ExtAttrSelect - процедура сбор сумм по доп.атрибутам для расчета ПСК
==========================================================================================
========================================================================================== */
select 1
`
	result, err := parser.ParseContent(content)
	if err != nil {
		t.Fatalf("ParseContent: %v", err)
	}
	if len(result.Procedures) != 1 {
		t.Fatalf("procedures = %d, want 1", len(result.Procedures))
	}
	got := result.Procedures[0].Description
	if got != "Cons_ExtAttrSelect - процедура сбор сумм по доп.атрибутам для расчета ПСК" {
		t.Fatalf("description = %q", got)
	}
}

func TestParseContent_ProcedureDescription_TruncatedToLimit(t *testing.T) {
	parser := NewParser()
	long := strings.Repeat("слово ", 4000) // ~24 КБ
	content := "DCL_PROC_BEGIN(BigDesc_Proc)\nas\n/*\n" + long + "\n*/\nselect 1\n"
	result, err := parser.ParseContent(content)
	if err != nil {
		t.Fatalf("ParseContent: %v", err)
	}
	if len(result.Procedures) != 1 {
		t.Fatalf("procedures = %d, want 1", len(result.Procedures))
	}
	got := result.Procedures[0].Description
	if len(got) > 8*1024 {
		t.Fatalf("description len = %d, want <= 8192", len(got))
	}
	if !utf8ValidString(got) {
		t.Fatalf("description обрезан посреди руны")
	}
}

func TestParseContent_ProcedureDescription_LineCommentNotDescription(t *testing.T) {
	parser := NewParser()
	content := `DCL_PROC_BEGIN(LineComment_Proc)
as
-- это line-комментарий, не описание
select 1
`
	result, err := parser.ParseContent(content)
	if err != nil {
		t.Fatalf("ParseContent: %v", err)
	}
	if len(result.Procedures) != 1 {
		t.Fatalf("procedures = %d, want 1", len(result.Procedures))
	}
	if result.Procedures[0].Description != "" {
		t.Fatalf("description = %q, want пустая строка (-- не описание)", result.Procedures[0].Description)
	}
}

func TestParseContent_ProcedureDescription_SingleLineBlockComment(t *testing.T) {
	parser := NewParser()
	content := `create proc Simple_Proc
as
/* Simple_Proc - вспомогательная процедура для парсера */
select 1
`
	result, err := parser.ParseContent(content)
	if err != nil {
		t.Fatalf("ParseContent: %v", err)
	}
	if len(result.Procedures) != 1 {
		t.Fatalf("procedures = %d, want 1", len(result.Procedures))
	}
	if got := result.Procedures[0].Description; got != "Simple_Proc - вспомогательная процедура для парсера" {
		t.Fatalf("description = %q", got)
	}
}

// Межпроцедурная утечка: комментарий-разделитель между процедурами не должен
// становиться описанием следующей процедуры. Конец процедуры — по FA-конвенции
// __END_PROCEDURE__ (X_ANYMODE идёт после конца и процедурой не является).
func TestParseContent_ProcedureDescription_NoLeakAcrossProcedures(t *testing.T) {
	parser := NewParser()
	content := `create proc First_Proc
as
/* First_Proc - описание первой процедуры */
begin
  select 1
end
__END_PROCEDURE__(First_Proc)

/*==========================================================
  Разделитель двух процедур в общем скрипте
==========================================================*/

create proc Second_Proc
as
begin
  select 2
end
__END_PROCEDURE__(Second_Proc)`
	result, err := parser.ParseContent(content)
	if err != nil {
		t.Fatalf("ParseContent: %v", err)
	}
	if len(result.Procedures) != 2 {
		t.Fatalf("procedures = %d, want 2", len(result.Procedures))
	}
	if got := result.Procedures[0].Description; got != "First_Proc - описание первой процедуры" {
		t.Fatalf("первая процедура description = %q", got)
	}
	if got := result.Procedures[1].Description; got != "" {
		t.Fatalf("вторая процедура получила чужое описание: %q", got)
	}
}

// Утечка API-стиля: незарегистрированный API_CREATE_PROC(X) с комментарием
// не должен отдавать своё описание следующей DCL-процедуре Y.
func TestParseContent_ProcedureDescription_NoLeakFromUnregisteredAPIStyle(t *testing.T) {
	parser := NewParser()
	content := `API_CREATE_PROC(API_Ghost_Proc)
/* Описание призрачной процедуры, которая не будет зарегистрирована */
DCL_PROC_BEGIN(Real_Proc)
as
select 1
end`
	result, err := parser.ParseContent(content)
	if err != nil {
		t.Fatalf("ParseContent: %v", err)
	}
	var real *struct {
		Name string
		Desc string
	}
	for _, proc := range result.Procedures {
		if proc.ProcName == "Real_Proc" {
			real = &struct {
				Name string
				Desc string
			}{proc.ProcName, proc.Description}
		}
	}
	if real == nil {
		t.Fatalf("Real_Proc не найден, procedures=%d", len(result.Procedures))
	}
	if real.Desc != "" {
		t.Fatalf("Real_Proc получил чужое описание: %q", real.Desc)
	}
}

func utf8ValidString(s string) bool {
	return utf8.ValidString(s)
}
