package store

import "testing"

// Мojibake-текст (CP1251-файл, декодированный как CP866): «Процедура» и
// «Входные» в CP866-одежде — рамки и спецсимволы.
func TestIsMojibakeText_Positive(t *testing.T) {
	cases := []string{
		"\u2554\u2569\u2557\u2560\u2551\u2554\u2560\u2550 procedure", // ╔ ╩ ╗ ╠ ║ ...
		"╧ЁюЎхфєЁр API_Audit_FindFrstActByObjtLst",                   // Процедура
		"┬їюфэ√х ярЁрьхЄЁ√",                                          // Входные параметры
		"хёыш чэрўхэшх Єю фюъєьхэЄ",                                  // если значение то документ
		"Сохранение информации ──────────",
	}
	for _, tc := range cases {
		if !isMojibakeText(tc) {
			t.Fatalf("isMojibakeText(%q) = false, want true", tc)
		}
	}
}

// Все-кириллический mojibake (CP1251 строчные а-п дают обычные руны р-я):
// артефакт-рун нет, ловится обратной транслитерацией + маркерами русского.
func TestIsMojibakeText_AllCyrillicPositive(t *testing.T) {
	cases := []string{
		"яюшеър фюъєьхэЄр фыя яюшёър", // точка документа для поиска
		"яетЁрыЄ ёєьь шч арэър ярЁЄэхЁр", // возврат сумм из банка партнера
		"шфхэЄшушърЄюЁ фюъєьхэЄр фюуюфюър", // идентификатор документа договора
	}
	for _, tc := range cases {
		if !isMojibakeText(tc) {
			t.Fatalf("isMojibakeText(%q) = false, want true (all-cyrillic mojibake)", tc)
		}
	}
}

// Легитимные русские описания (и латиница с цифрами) — не mojibake.
func TestIsMojibakeText_Negative(t *testing.T) {
	cases := []string{
		"ReturnCashFund_Insert - возврат сумм из Банка-партнера на счет клиента",
		"Метод осуществляет привязку кредитных договоров к классификатору.",
		"API_CCred_BindClassifier @ContractID DSIDENTIFIER",
		"№ счёта, сумма 100.50 руб., температура °C", // № и ° — легитимны (CP866 0xFC/0xF8)
		"Перенос документа в архив по окончании срока, поиск по списку значений",
		"",
	}
	for _, tc := range cases {
		if isMojibakeText(tc) {
			t.Fatalf("isMojibakeText(%q) = true, want false", tc)
		}
	}
}
