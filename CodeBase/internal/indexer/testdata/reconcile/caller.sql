create procedure CallerA
as
begin
	exec CalleeB
	select AccountID from tAccount
end

