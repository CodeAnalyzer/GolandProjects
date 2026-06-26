package retcode

import (
	"regexp"
	"strconv"
	"strings"

	"github.com/codebase/internal/model"
)

// Паттерн 1: exec ReturnCode_Insert <code>, <CONST|'msg'>, 'ProcName'
var reRetCodeInsert = regexp.MustCompile(
	`(?is)ReturnCode_Insert\s+(\d+)\s*,\s*([A-Za-z_][A-Za-z0-9_]*|'[^']*')\s*,\s*'([^']*)'`)

// Паттерн 2: макрос _ADD_RETCODE_(code, 'msg', 'proc') или M_*_RETCODE_INSERT(code, 'msg', 'proc')
// 3 аргумента: numeric, 'literal', 'proc'
var reRetCodeMacro3 = regexp.MustCompile(
	`(?i)\w+\s*\(\s*(\d+)\s*,\s*'([^']+)'\s*,\s*'([^']+)'\s*\)`)

// Паттерн 2b: __Notification_Save(code, 'msg', 'proc', DSMODULE_*_ID)
// 4 аргумента: numeric, 'literal', 'proc', module_id
var reRetCodeMacro4 = regexp.MustCompile(
	`(?i)\w+\s*\(\s*(\d+)\s*,\s*'([^']+)'\s*,\s*'([^']*)'\s*,\s*(\w+)\s*\)`)

// Прескрининг: быстрая проверка наличия return code паттернов
var rePrescreen = regexp.MustCompile(
	`(?i)(ReturnCode_Insert|_ADD_RETCODE_|RETCODE_INSERT|__Notification_Save|FCD_\w+_Notification_Save)`)

// HasReturnCodes — быстрая проверка содержимого на наличие return code паттернов.
func HasReturnCodes(content string) bool {
	return rePrescreen.MatchString(content)
}

// Parse извлекает return code записи из содержимого SQL-файла.
func Parse(content string) []*model.RetCodeEntry {
	var entries []*model.RetCodeEntry

	// Паттерн 1: ReturnCode_Insert
	for _, m := range reRetCodeInsert.FindAllStringSubmatch(content, -1) {
		code, _ := strconv.ParseInt(m[1], 10, 64)
		arg2 := strings.TrimSpace(m[2])
		procName := strings.TrimSpace(m[3])
		isConst := !strings.HasPrefix(arg2, "'")
		msg := arg2
		if !isConst {
			msg = strings.Trim(arg2, "'")
		}
		entries = append(entries, &model.RetCodeEntry{
			RetCode:    code,
			Message:    msg,
			ProcName:   procName,
			ModuleID:   int(code / 10000),
			IsConstant: isConst,
		})
	}

	// Паттерн 2b: 4-арг макросы (например __Notification_Save)
	matched4 := make(map[int64]bool)
	for _, m := range reRetCodeMacro4.FindAllStringSubmatch(content, -1) {
		code, _ := strconv.ParseInt(m[1], 10, 64)
		msg := strings.TrimSpace(m[2])
		procName := strings.TrimSpace(m[3])
		moduleArg := strings.TrimSpace(m[4])
		moduleID := resolveModuleID(moduleArg, code)
		matched4[code] = true
		entries = append(entries, &model.RetCodeEntry{
			RetCode:    code,
			Message:    msg,
			ProcName:   procName,
			ModuleID:   moduleID,
			IsConstant: false,
		})
	}

	// Паттерн 2: 3-арг макросы (но не те, что уже匹配лись как 4-арг)
	for _, m := range reRetCodeMacro3.FindAllStringSubmatch(content, -1) {
		code, _ := strconv.ParseInt(m[1], 10, 64)
		if matched4[code] {
			continue
		}
		msg := strings.TrimSpace(m[2])
		procName := strings.TrimSpace(m[3])
		entries = append(entries, &model.RetCodeEntry{
			RetCode:    code,
			Message:    msg,
			ProcName:   procName,
			ModuleID:   int(code / 10000),
			IsConstant: false,
		})
	}

	return entries
}

// resolveModuleID извлекает module ID из 4-го аргумента макроса.
// Если аргумент — числовое значение, используется оно.
// Если аргумент — DSMODULE_*_ID константа, извлекаем число из имени.
// Иначе fallback: ret_code / 10000.
func resolveModuleID(arg string, retCode int64) int {
	arg = strings.TrimSpace(arg)
	if n, err := strconv.Atoi(arg); err == nil {
		return n
	}
	// Пытаемся извлечь число из DSMODULE_*_ID
	if strings.HasPrefix(strings.ToUpper(arg), "DSMODULE_") {
		// DSMODULE_CONSUMER_EXT_ID → 76 (из DEALMAIN.H)
		if id, ok := moduleConstantMap[strings.ToUpper(arg)]; ok {
			return id
		}
	}
	return int(retCode / 10000)
}

// moduleConstantMap — маппинг DSMODULE_*_ID констант в числовые значения.
// Источник: fa-commonfiles/CommonFiles/SERVER/Include/DEALMAIN.H
var moduleConstantMap = map[string]int{
	"DSMODULE_CORE_ID":           10,
	"DSMODULE_SEC_ID":            11,
	"DSMODULE_ESEC_ID":           12,
	"DSMODULE_MONEYMARKET_EXT_ID": 13,
	"DSMODULE_MBK_ID":            14,
	"DSMODULE_MONEYMARKET_ID":    15,
	"DSMODULE_BILL_ID":           16,
	"DSMODULE_LOAN_ID":           17,
	"DSMODULE_DEPO_ID":           18,
	"DSMODULE_OFBU_ID":           19,
	"DSMODULE_DEPOSIT_ID":        20,
	"DSMODULE_RKO_ID":            21,
	"DSMODULE_CED_ID":            22,
	"DSMODULE_FUT_ID":            23,
	"DSMODULE_CB_ID":             24,
	"DSMODULE_HYP_ID":            25,
	"DSMODULE_EXP_ID":            26,
	"DSMODULE_COST_ID":           27,
	"DSMODULE_TRUST_ID":          28,
	"DSMODULE_CARD_ID":           29,
	"DSMODULE_METAL_ID":          30,
	"DSMODULE_CASH_ID":           31,
	"DSMODULE_CASHDOC_ID":        32,
	"DSMODULE_DDM_ID":            33,
	"DSMODULE_SAFE_ID":           34,
	"DSMODULE_EXACT_ID":          35,
	"DSMODULE_WARRANTY_ID":       36,
	"DSMODULE_RMS_ID":            37,
	"DSMODULE_CONSUMER_ID":       39,
	"DSMODULE_TAX_ID":            40,
	"DSMODULE_TRANSFER_ID":       41,
	"DSMODULE_CR_ID":             42,
	"DSMODULE_MARGINTRD_ID":      43,
	"DSMODULE_LIMIT_ID":          44,
	"DSMODULE_FACTORING_ID":      45,
	"DSMODULE_CURRREG_ID":        46,
	"DSMODULE_UNDERWRITE_ID":     47,
	"DSMODULE_BIZONE_ID":         48,
	"DSMODULE_DOCCREDIT_ID":      49,
	"DSMODULE_GL_ID":             50,
	"DSMODULE_RETAILCTR_ID":      51,
	"DSMODULE_PAYINSTR_ID":       52,
	"DSMODULE_BCH_ID":            53,
	"DSMODULE_REPORTS_ID":        54,
	"DSMODULE_RATING_ID":         55,
	"DSMODULE_USERPROC_ID":       56,
	"DSMODULE_ARCHIVER_ID":       57,
	"DSMODULE_RESERVEPORTFOLIO_ID": 58,
	"DSMODULE_FRONTGATE_ID":      59,
	"DSMODULE_MESSSERVER_ID":     60,
	"DSMODULE_CCNTRLCORE_ID":     61,
	"DSMODULE_CBA_ID":            62,
	"DSMODULE_GWCLBANK_ID":       63,
	"DSMODULE_SMCORE_ID":         64,
	"DSMODULE_EARCHDOC_ID":       65,
	"DSMODULE_NETTING_EXT_ID":    66,
	"DSMODULE_POOLMGR_ID":        67,
	"DSMODULE_SECURITYISSUE_ID":  68,
	"DSMODULE_RAPIDA_ID":         69,
	"DSMODULE_QIWI_ID":           70,
	"DSMODULE_EXCHANGERATE_ID":   71,
	"DSMODULE_DEPOSIT_EXT_ID":    72,
	"DSMODULE_STOCKMARKET_EXT_ID": 73,
	"DSMODULE_INOUTDOC_EXT_ID":   74,
	"DSMODULE_SMCORE_EXT_ID":     75,
	"DSMODULE_CONSUMER_EXT_ID":   76,
	"DSMODULE_INTEGRATES_ID":     77,
	"DSMODULE_GAAP_ID":           78,
	"DSMODULE_PAYCENTRE_ID":      79,
	"DSMODULE_FICORE_ID":         80,
	"DSMODULE_INOUTDOC_ID":       81,
	"DSMODULE_POOLING_ID":        82,
	"DSMODULE_IR_CC_SWAP_ID":     83,
	"DSMODULE_FINCONTROL_ID":     84,
	"DSMODULE_CREDITCORE_ID":     85,
	"DSMODULE_AML_ID":            86,
	"DSMODULE_CTRTRNSF_ID":       87,
	"DSMODULE_CONSTANT_ID":       88,
	"DSMODULE_TESTAPI_ID":        89,
	"DSMODULE_EMS_ID":            90,
	"DSMODULE_ACCRUALCORE_ID":    91,
	"DSMODULE_COMMON_ID":         92,
	"DSMODULE_SYSTEMMONITORING_ID": 93,
	"DSMODULE_CRNCONTROL_ID":     94,
	"DSMODULE_INVOICE_ID":        95,
	"DSMODULE_NETTING_ID":        205,
	"DSMODULE_FIACCOUNTING_ID":   400,
	"DSMODULE_INSURANCE_EXT_ID":  405,
}
