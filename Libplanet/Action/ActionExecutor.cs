namespace Libplanet.Action
{
    public delegate IAccountStateDelta ActionExecutor(IActionContext context, IAction action);
}
