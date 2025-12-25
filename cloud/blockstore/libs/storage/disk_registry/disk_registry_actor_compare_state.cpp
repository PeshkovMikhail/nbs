#include "disk_registry_actor.h"

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

namespace {

class TCompareActor final: public TActorBootstrapped<TCompareActor>
{
private:
    const TChildLogTitle LogTitle;
    const TActorId Owner;
    TRequestInfoPtr RequestInfo;

public:
    TCompareActor(
        TChildLogTitle logTitle,
        const TActorId& owner,
        TRequestInfoPtr requestInfo);

    void Bootstrap(const TActorContext& ctx);

private:
    void ReplyAndDie(
        const TActorContext& ctx,
        NProto::TCompareDiskRegistryStateWithLocalDbResponse& responseProto);

private:
    STFUNC(StateCompare);

    void HandleBackupDiskRegistryStateResponse(
        const TEvDiskRegistry::TEvBackupDiskRegistryStateResponse::TPtr& ev,
        const TActorContext& ctx);
};

TCompareActor::TCompareActor(
    TChildLogTitle logTitle,
    const TActorId& owner,
    TRequestInfoPtr requestInfo)
    : LogTitle(std::move(logTitle))
    , Owner(owner)
    , RequestInfo(std::move(requestInfo))
{}

void TCompareActor::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateCompare);

    auto request =
        std::make_unique<TEvDiskRegistry::TEvBackupDiskRegistryStateRequest>();
    request->Record.SetSource(NProto::EBackupDiskRegistryStateSource::BOTH);

    NCloud::Send(ctx, Owner, std::move(request));
}

void TCompareActor::ReplyAndDie(
    const TActorContext& ctx,
    NProto::TCompareDiskRegistryStateWithLocalDbResponse& responseProto)
{
    auto response = std::make_unique<
        TEvDiskRegistry::TEvCompareDiskRegistryStateWithLocalDbResponse>(
        responseProto);
    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TCompareActor::HandleBackupDiskRegistryStateResponse(
    const TEvDiskRegistry::TEvBackupDiskRegistryStateResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    NProto::TCompareDiskRegistryStateWithLocalDbResponse response;

    google::protobuf::util::MessageDifferencer diff;

    diff.ReportDifferencesToString(response.MutableDiffers());
    google::protobuf::util::DefaultFieldComparator comparator;
    comparator.set_float_comparison(
        google::protobuf::util::DefaultFieldComparator::FloatComparison::
            APPROXIMATE);
    diff.set_field_comparator(&comparator);

    diff.IgnoreField(
        NProto::TAgentConfig::descriptor()->FindFieldByName("UnknownDevices"));

    diff.Compare(msg->Record.GetRamBackup(), msg->Record.GetLocalDbBackup());

    auto report = response.GetDiffers();

    LOG_INFO(
        ctx,
        TBlockStoreComponents::DISK_REGISTRY,
        "%s CompareDiskRegistryStateWithLocalDb result: %s",
        LogTitle.GetWithTime().c_str(),
        report.empty() ? "OK" : report.c_str());

    ReplyAndDie(ctx, response);
}

STFUNC(TCompareActor::StateCompare)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(
            TEvDiskRegistry::TEvBackupDiskRegistryStateResponse,
            HandleBackupDiskRegistryStateResponse);
        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::DISK_REGISTRY_WORKER,
                __PRETTY_FUNCTION__);
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryActor::HandleCompareDiskRegistryStateWithLocalDb(
    const TEvDiskRegistry::TEvCompareDiskRegistryStateWithLocalDbRequest::TPtr&
        ev,
    const TActorContext& ctx)
{
    BLOCKSTORE_DISK_REGISTRY_COUNTER(CompareDiskRegistryStateWithLocalDb);

    const auto* msg = ev->Get();

    LOG_INFO(
        ctx,
        TBlockStoreComponents::DISK_REGISTRY,
        "%s Received CompareDiskRegistryStateWithLocalDb request",
        LogTitle.GetWithTime().c_str());

    auto actor = NCloud::Register<TCompareActor>(
        ctx,
        LogTitle.GetChildWithTags(GetCycleCount(), {}),
        ctx.SelfID,
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext));
    Actors.insert(actor);
}

}   // namespace NCloud::NBlockStore::NStorage
