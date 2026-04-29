// Package sagaio собирает константы топиков и proto-helpers, которыми
// пользуются все 5 main.go этой лекции. Решено вынести наружу, чтобы не
// размазывать одни и те же строковые литералы и unmarshal-обвязку по пяти
// файлам — иначе любая опечатка в имени топика становится тихим багом.
package sagaio

// Choreography — отдельные топики на каждое событие. Сервисы подписываются
// только на те, что им нужны: payment слушает order.requested и события
// компенсации, inventory — payment.completed и shipment.failed, и т.д.
const (
	TopicChoreoOrderRequested    = "lecture-06-05-choreo.order-requested"
	TopicChoreoPaymentCompleted  = "lecture-06-05-choreo.payment-completed"
	TopicChoreoPaymentFailed     = "lecture-06-05-choreo.payment-failed"
	TopicChoreoPaymentRefunded   = "lecture-06-05-choreo.payment-refunded"
	TopicChoreoInventoryReserved = "lecture-06-05-choreo.inventory-reserved"
	TopicChoreoInventoryFailed   = "lecture-06-05-choreo.inventory-failed"
	TopicChoreoInventoryReleased = "lecture-06-05-choreo.inventory-released"
	TopicChoreoShipmentScheduled = "lecture-06-05-choreo.shipment-scheduled"
	TopicChoreoShipmentFailed    = "lecture-06-05-choreo.shipment-failed"
)

// Orchestration — пары cmd/reply на каждый сервис плюс topик-вход place-order.
// Оркестратор слушает все *-reply, сервисы — свой *-cmd.
const (
	TopicOrchPlaceOrder     = "lecture-06-05-orch.place-order"
	TopicOrchPaymentCmd     = "lecture-06-05-orch.payment-cmd"
	TopicOrchPaymentReply   = "lecture-06-05-orch.payment-reply"
	TopicOrchInventoryCmd   = "lecture-06-05-orch.inventory-cmd"
	TopicOrchInventoryReply = "lecture-06-05-orch.inventory-reply"
	TopicOrchShipmentCmd    = "lecture-06-05-orch.shipment-cmd"
	TopicOrchShipmentReply  = "lecture-06-05-orch.shipment-reply"
)

// AllTopics — список для make topic-create-all. Один список, чтобы сервисы
// при тесте могли пересоздать все топики разом.
func AllTopics() []string {
	return []string{
		TopicChoreoOrderRequested,
		TopicChoreoPaymentCompleted,
		TopicChoreoPaymentFailed,
		TopicChoreoPaymentRefunded,
		TopicChoreoInventoryReserved,
		TopicChoreoInventoryFailed,
		TopicChoreoInventoryReleased,
		TopicChoreoShipmentScheduled,
		TopicChoreoShipmentFailed,
		TopicOrchPlaceOrder,
		TopicOrchPaymentCmd,
		TopicOrchPaymentReply,
		TopicOrchInventoryCmd,
		TopicOrchInventoryReply,
		TopicOrchShipmentCmd,
		TopicOrchShipmentReply,
	}
}
